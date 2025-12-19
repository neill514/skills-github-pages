package com.hmdp.service.impl;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.RedisConstants;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.Resource;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
   @Resource
   private StringRedisTemplate stringRedisTemplate;

    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);
        //互斥锁解决缓存击穿
        Shop shop = queryWithMutex(id);
        if(shop == null){
            return Result.fail("店铺不存在!");
        }
        //返回
        return Result.ok(shop);
    }

    //获取锁
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    //释放锁
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }

    @Override
    @Transactional
    public Result update(Shop shop) {
        Long id = shop.getId();
        if(id == null){
            return Result.fail("店铺id为空");
        }
        //更新数据库
        updateById(shop);

        //删除缓存
        stringRedisTemplate.delete(RedisConstants.CACHE_SHOP_KEY+id);
        return Result.ok();
    }

    public Shop queryWithMutex(Long id){
        //从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY+id);
        //判断是否存在
        //代表null, " "空字符串, "\t\n" 为false
        if(StrUtil.isNotBlank(shopJson)){
            //存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        //判断命中的是否是空值
        //shopJson不为null,代表为" "
        if(shopJson!=null){
            return null;
        }

        //不存在，实现缓存重建
        //1.获取互斥锁
        String lockKey="lock:shop:"+id;
        Shop shop = null;
        try {
            boolean isLock = tryLock(lockKey);
            //2.判断是否获取成功
            if(!isLock){
                //3.失败，则休眠重试
                Thread.sleep(50);
                return queryWithMutex(id);
            }

            //4.成功，根据id查询数据库
            shop = getById(id);
            //模拟重建的延时
            Thread.sleep(200);
            //不存在，返回错误
            if(shop == null){
                //将空值写入redis
                stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //存在，写入redis
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally{
            //释放互斥锁
            unlock(lockKey);
        }
        //返回
        return shop;
    }


    public Shop queryWithPassThrough(Long id){
        //从redis查询商铺缓存
        String shopJson = stringRedisTemplate.opsForValue().get(RedisConstants.CACHE_SHOP_KEY+id);
        //判断是否存在
        //代表null, " "空字符串, "\t\n" 为false
        if(StrUtil.isNotBlank(shopJson)){
            //存在，直接返回
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        //判断命中的是否是空值
        //shopJson不为null,代表为" "
        if(shopJson!=null){
            return null;
        }

        //不存在，根据id查询数据库
        Shop shop = getById(id);
        //不存在，返回错误
        if(shop == null){
            //将空值写入redis
            stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,"",RedisConstants.CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //存在，写入redis
        stringRedisTemplate.opsForValue().set(RedisConstants.CACHE_SHOP_KEY+id,JSONUtil.toJsonStr(shop),RedisConstants.CACHE_SHOP_TTL, TimeUnit.MINUTES);
        //返回
        return shop;
    }
}
