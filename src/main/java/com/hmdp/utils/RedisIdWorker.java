package com.hmdp.utils;

import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;

//全局唯一ID生成器
@Component
public class RedisIdWorker {
    //开始时间戳
    private static final long BEGIN_TIMESTAMP=1640995200L;
    private static final long COUNT_BITS=32;

    private StringRedisTemplate stringRedisTemplate;

    public RedisIdWorker(StringRedisTemplate stringRedisTemplate) {
        this.stringRedisTemplate = stringRedisTemplate;
    }

    public long nextId(String keyPrefix){
        //生成时间戳
        LocalDateTime now = LocalDateTime.now();
        long nowSecond = now.toEpochSecond(ZoneOffset.UTC);
        long timestamp=nowSecond-BEGIN_TIMESTAMP;
        //生成序列号
        //获取当前日期，精确到天
        String date=now.format(DateTimeFormatter.ofPattern("yyyy:MM:dd"));
        //自增长
        long count=stringRedisTemplate.opsForValue().increment("icr:"+keyPrefix+":"+date);
        //拼接并返回
        return timestamp<<COUNT_BITS|count;//timestamp向左移动32位，count在后面自增长
    }

    public static void main(String[] args) {
        LocalDateTime time = LocalDateTime.of(2025, 1, 1, 0, 0, 0);
        long second = time.toEpochSecond(ZoneOffset.UTC);//依靠时区转为秒数
        System.out.println("second="+second);
    }
}
