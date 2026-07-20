package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.aop.framework.AopContext;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.connection.stream.*;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/*
* 防超卖+异步秒杀思路
* 方法启动后，首先初始化，创建一个单线程池+runnable任务，实现异步线程，首先用户发起请求，利用redis里面的自增长方法，生成一个订单id,
* 然后执行Lua脚本（传入优惠券id，用户id,订单id）,在里面判断用户是否重复下单以及库存充足，返回（0，1，2）,如果为O,
* 在Lua脚本里面扣库存和下单（对redis操作）,为0后，出来在方法里封装数据创建订单放入阻塞队列，并创建代理对象。
* 异步线程任务是从阻塞队列里面读取订单信息，读取成功后就可以下单，在下单方法里面实现可重入锁，防止自己嵌套以及重复下单。
* 因为是扣除数据库和创建订单在异步线程中，但必须同时成功，需调用事务。异步线程没有代理对象，所以通过主线程的代理对象实现事务，创建订单
* 并扣库存（对数据库操作）.为了防止服务器断电，redis宕机等情况，在异步线程里面捕获异常，再次实现获取阻塞队列里面的订单信息
* */

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
@Slf4j
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    @Resource
    private ISeckillVoucherService seckillVoucherService;

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    @Resource
    private RedisIdWorker redisIdWorker;

    @Resource
    private RedissonClient redissonClient;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    //主线程获取到的代理对象，供异步线程调用以开启事务
    private IVoucherOrderService proxy;

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }

    /**
     * 异步处理消息队列（Redis Stream）中的订单
     */
    private class VoucherOrderHandler implements Runnable {
        String queueName = "stream.orders";

        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(queueName, ReadOffset.lastConsumed())
                    );
                    //2.判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        //2.1.获取失败，重新获取
                        continue;
                    }
                    //3.解析消息中的订单消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //4.如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    //5.ACK确认 XACK stream.orders g1 消息id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                    handlePendingList();
                }
            }
        }

        private void handlePendingList() {
            while (true) {
                try {
                    //1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 count 1 STREAMS stream.orders 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        //2.1.获取失败，说明pending-list没有异常消息
                        Thread.sleep(1000);
                        continue;
                    }
                    //3.解析消息中的订单消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //4.如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    //5.ACK确认 XACK stream.orders g1 消息id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());
                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException ignored) {
                    }
                }
            }
        }
    }

    private void handleVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取可重入锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if (!isLock) {
            log.error("不允许重复下单");
            return;
        }
        try {
            //通过代理对象调用，保证事务生效
            proxy.createVoucherOrder(voucherOrder);
        } finally {
            lock.unlock();
        }
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //1.订单id
        long orderId = redisIdWorker.nextId("order");
        //2.执行Lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString(), String.valueOf(orderId)
        );
        //3.判断结果是否为0
        int r = result.intValue();
        if (r != 0) {
            //3.1.不为0，代表没有购买资格
            return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
        }
        //3.2.为0，代表有购买资格，订单已由Lua脚本写入消息队列
        //4.获取代理对象（事务）
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //5.返回订单id
        return Result.ok(orderId);
    }

    @Transactional
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        //查询订单（一人一单兜底校验）
        int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
        if (count > 0) {
            log.error("用户已经购买过一单");
            return;
        }

        //扣减库存
        boolean success = seckillVoucherService.update()
                .setSql("stock=stock-1")//set stock=stock-1
                .eq("voucher_id", voucherOrder.getVoucherId())
                .gt("stock", 0)//where voucher_id=? && stock>0
                .update();
        if (!success) {
            log.error("库存不足");
            return;
        }

        //创建订单
        save(voucherOrder);
    }
}
