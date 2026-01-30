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
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
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
        SECKILL_SCRIPT=new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }


    private static final ExecutorService SECKILL_ORDER_EXECUTOR= Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandle());
    }
    private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    private class VoucherOrderHandle implements Runnable {
        String queueName = "stream.order";

        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 BLOCK 2000 STREAMS streams.order >
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
                    //3.如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 消息id
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
                    //1.获取pending-list中的订单信息 XREADGROUP GROUP g1 c1 count 1 STREAMS streams.order 0
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from("g1", "c1"),
                            StreamReadOptions.empty().count(1),
                            StreamOffset.create(queueName, ReadOffset.from("0"))
                    );
                    //2.判断消息是否获取成功
                    if (list == null || list.isEmpty()) {
                        //2.1.获取失败，说明pending-list没有异常消息
                        break;
                    }
                    //3.解析消息中的订单消息
                    MapRecord<String, Object, Object> record = list.get(0);
                    Map<Object, Object> values = record.getValue();
                    VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(values, new VoucherOrder(), true);
                    //3.如果获取成功，可以下单
                    handleVoucherOrder(voucherOrder);
                    //4.ACK确认 SACK stream.orders g1 消息id
                    stringRedisTemplate.opsForStream().acknowledge(queueName, "g1", record.getId());

                } catch (Exception e) {
                    log.error("处理pending-list订单异常", e);
                }
            }
        }
    }



    /*private BlockingQueue<VoucherOrder> orderTasks=new ArrayBlockingQueue<>(1024*1024);
    private class VoucherOrderHandle implements Runnable {
        @Override
        public void run() {
            while (true) {
                try {
                    //1.获取阻塞队列中的订单信息
                    VoucherOrder voucherOrder = orderTasks.take();
                    //2.创建订单
                    handleVoucherOrder(voucherOrder);
                } catch (Exception e) {
                    log.error("处理订单异常", e);
                }
            }
        }
    }*/

        private void handleVoucherOrder(VoucherOrder voucherOrder) {
            Long userId = voucherOrder.getUserId();
            //创建锁对象
            //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
            RLock lock = redissonClient.getLock("lock:order:" + userId);
            //获取锁
            boolean isLock = lock.tryLock();
            //判断是否获取锁成功
            if (!isLock) {
                log.error("不允许重复下单");
                return;
            }
            try {
                //获取代理对象(事务)
                proxy.createVoucherOrder(voucherOrder);
            } finally {
                lock.unlock();
            }
        }

    /*@Override
    @Transactional
    public Result seckillVoucher(Long voucherId) {
        //查询优惠券
        SeckillVoucher voucher = seckillVoucherService.getById(voucherId);
        //判断秒杀是否开始
        if (voucher.getBeginTime().isAfter(LocalDateTime.now())) {
            //尚未开始
            return Result.fail("秒杀尚未开始!");
        } 
        //判断秒杀是否已经结束
        if (voucher.getEndTime().isBefore(LocalDateTime.now())) {
            //尚未开始
            return Result.fail("秒杀已经结束!");
        }
        //判断库存是否充足
        if(voucher.getStock()<1){
            return Result.fail("库存不足");
        }

        //一人一单
        Long userId = UserHolder.getUser().getId();
        //创建锁对象
        //SimpleRedisLock lock = new SimpleRedisLock("order:" + userId, stringRedisTemplate);
        RLock lock = redissonClient.getLock("lock:order:" + userId);
        //获取锁
        boolean isLock = lock.tryLock();
        //判断是否获取锁成功
        if (!isLock) {
            return Result.fail("不允许重复下单");
        }
        try {
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);
        } finally {
            lock.unlock();
        }

        /*synchronized (userId.toString ().intern()) {
            //获取代理对象（事务）
            IVoucherOrderService proxy = (IVoucherOrderService) AopContext.currentProxy();
            return proxy.createVoucherOrder(voucherId);}
 }*/

        private IVoucherOrderService proxy;

        @Override
        public Result seckillVoucher(Long voucherId) {
            Long userId = UserHolder.getUser().getId();
            //2.3.订单id
            long orderId = redisIdWorker.nextId("order");
            //1.执行Lua脚本
            Long result = stringRedisTemplate.execute(
                    SECKILL_SCRIPT,
                    Collections.emptyList(),
                    voucherId.toString(), userId.toString(), String.valueOf(orderId)
            );
            //2.判断结果是否为0
            int r = result.intValue();
            if (r != 0) {
                //2.1.不为0，代表没有购买资格
                return Result.fail(r == 1 ? "库存不足" : "不能重复下单");
            }
            //2.2.为0，代表有购买资格，把下单信息保存到阻塞队列中
            //创建订单
            VoucherOrder voucherOrder = new VoucherOrder();

            voucherOrder.setId(orderId);
            //2.4.用户id
            voucherOrder.setUserId(userId);
            //2.5.代金券id
            voucherOrder.setVoucherId(voucherId);
            //2.6.放入阻塞队列
            orderTasks.add(voucherOrder);

            //3.获取代理对象
            //获取代理对象(事务)
            proxy = (IVoucherOrderService) AopContext.currentProxy();
            //3.返回订单id
            return Result.ok(orderId);
        }
   /* @Override
    public Result seckillVoucher(Long voucherId) {
        Long userId = UserHolder.getUser().getId();
        //1.执行Lua脚本
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(), userId.toString()
        );
        //2.判断结果是否为0
        int r = result.intValue();
        if(r!=0){
            //2.1.不为0，代表没有购买资格
            return Result.fail(r==1?"库存不足":"不能重复下单");
         }
        //2.2.为0，代表有购买资格，把下单信息保存到阻塞队列中
        //创建订单
        VoucherOrder voucherOrder = new VoucherOrder();
        //2.3.订单id
        long orderId = redisIdWorker.nextId("order");
        voucherOrder.setId(orderId);
        //2.4.用户id
        voucherOrder.setUserId(userId);
        //2.5.代金券id
        voucherOrder.setVoucherId(voucherId);
        //2.6.放入阻塞队列
        orderTasks.add(voucherOrder);

        //3.获取代理对象
        //获取代理对象(事务)
        proxy = (IVoucherOrderService) AopContext.currentProxy();
        //3.返回订单id
        return Result.ok(orderId);
    }*/

        @Transactional
        public void createVoucherOrder(VoucherOrder voucherOrder) {
            Long userId = voucherOrder.getUserId();
            //查询订单
            int count = query().eq("user_id", userId).eq("voucher_id", voucherOrder.getVoucherId()).count();
            if (count > 0) {
                log.error("用户已经购买过一单");
                return;
            }

            //扣减库存
            boolean success = seckillVoucherService.update()
                    .setSql("stock=stock-1")//set stock=stock-1
                    .eq("voucher_id", voucherOrder.getVoucherId())
                    .gt("stock", 0)//where id=? && stock=?
                    .update();
            if (!success) {
                log.error("库存不足");
                return;
            }

            //创建订单
            save(voucherOrder);
        }
    }

