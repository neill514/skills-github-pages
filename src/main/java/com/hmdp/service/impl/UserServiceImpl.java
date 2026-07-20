package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.bean.copier.CopyOptions;
import cn.hutool.core.lang.UUID;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import com.hmdp.mapper.UserMapper;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.data.redis.connection.BitFieldSubCommands;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import javax.servlet.http.HttpSession;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.SystemConstants.USER_NICK_NAME_PREFIX;

/**
 * <p>
 * 服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@Service
public class UserServiceImpl extends ServiceImpl<UserMapper, User> implements IUserService {

    @Resource
    private StringRedisTemplate stringRedisTemplate;

    public Result sendCode(String phone, HttpSession session) {
        //校验手机号
        if(RegexUtils.isPhoneInvalid(phone)) {
            //如果不符合，返回错误信息
            return Result.fail("手机号格式错误!");
        }
        //如果符合，生成验证码
        String code = RandomUtil.randomNumbers(6);

        //保存验证码到session
       /* session.setAttribute("code", code);*/

        //保存验证码到Redis
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY +phone,code, RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);
        //发送验证码
        log.debug("发送短信验证码:{}", code);
        return Result.ok();
    }
    /*
    * if(RegixUtilRegexUtils.isPhoneInvalid(phone))
    * {
    *  return result.fail("格式错误");
    * }
    * String code=RandomUtil.randomNumber(6);
    * stringRedisTemplate.opsForValue().set(phone,code,ttl,ttlouttime);
    * log.info("{}",code);
    * return ;
    * */


    public Result login(LoginFormDTO loginForm, HttpSession session) {
        //校验手机号
        String phone = loginForm.getPhone();
        if(RegexUtils.isPhoneInvalid(phone)) {
            //如果不符合，返回错误信息
            return Result.fail("手机号格式错误!");
        }
        //从session中获取，校验验证码
        /*Object cacheCode = session.getAttribute("code");*/
        //从Redis中获取，校验验证码
        String cacheCode= stringRedisTemplate.opsForValue().get(RedisConstants.LOGIN_CODE_KEY+phone);
        String code = loginForm.getCode();
        if(cacheCode == null||!code.toString().equals(cacheCode)) {
            return Result.fail("验证码错误");
        }
        //一致，根据手机号查询用户
        User user = query().eq("phone", phone).one();
        //判断用户是否存在
        if(user == null) {
            //不存在，创建新用户并保存到Redis
           user=createUserWithPhone(phone);
        }

        //不存在，创建新用户并保存到session
        /*session.setAttribute("user", BeanUtil.copyProperties(user, UserDTO.class));*/

        //随机生成一个token,作为登录令牌
        String token=UUID.randomUUID().toString(true);

        //将User对象转为hashmap存储,通过DTO便于维护
        UserDTO userDTO=BeanUtil.copyProperties(user,UserDTO.class);
        Map<String, Object> userMap = BeanUtil.beanToMap(userDTO,new HashMap<>(), CopyOptions.create()
                .setIgnoreNullValue(true)
                .setFieldValueEditor((fieldName,fieldValue)->fieldValue.toString()));

        //存储
        stringRedisTemplate.opsForHash().putAll(RedisConstants.LOGIN_USER_KEY+token,userMap);
        //设置token有效期
        stringRedisTemplate.expire(RedisConstants.LOGIN_USER_KEY+token,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
        //返回token到客户端
        return Result.ok(token);
    }

    @Override
    public Result sign() {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now =LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key=RedisConstants.USER_SIGN_KEY+userId+keySuffix;
        //4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.写入Redis SETBIT key offset 1
        stringRedisTemplate.opsForValue().setBit(key, dayOfMonth - 1, true);
        return Result.ok();
    }

    @Override
    public Result signCount() {
        //1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        //2.获取日期
        LocalDateTime now =LocalDateTime.now();
        //3.拼接key
        String keySuffix = now.format(DateTimeFormatter.ofPattern(":yyyyMM"));
        String key=RedisConstants.USER_SIGN_KEY+userId+keySuffix;
        //4.获取今天是本月的第几天
        int dayOfMonth = now.getDayOfMonth();
        //5.获取本月截止今天为止的所有的签到记录，返回的是一个十进制的数字 BITFIELD key GET u[dayOfMonth] 0
        List<Long> result = stringRedisTemplate.opsForValue().bitField(
                key,
                BitFieldSubCommands.create()
                        .get(BitFieldSubCommands.BitFieldType.unsigned(dayOfMonth))
                        .valueAt(0)
        );
        if(result==null||result.isEmpty()) {
            //没有任何签到结果
            return Result.ok(0);
        }
        Long num=result.get(0);
        if(num==null||num==0) {
            return Result.ok(0);
        }
        //6.循环遍历
        int count=0;
        while (true) {
            //6.1.让这个数字与1做与运算，得到数字的最后一个bit位 //判断这个bit位是否为0
            if ((num & 1)==0) {
                //如果为0，说明未签到，结束
                break;
            }else{
                //如果不为0，说明已签到，计数器+1
                count++;
            }
            //把数字右移一位，抛弃最后一个bit位，继续下一个bit位
            num >>>=1;
        }
        return Result.ok(count);
    }

    /*
       * if(RegixUtil.isphoneInvil(loginDto.getPhone))
       * {
       *  return Result.fail(no)
       * }
       *
       * cacheCode=stringRedisTemplate.opsforValue().get(phone);
       * code=loginDto.getCode();
       * if(cacheCode!=code||code==null){
       * return no;
       * }
       *
       * User  user=query().eq("phone",phone).one();
       * if(user==null)
       * {
       * User user=new User();
       * user.setName();
       * user.setNickName();
       * }
       * UserDTO userdto=StrUtil.fillBeanWithMap(user);
       * stringRedisTemplate.opsValue().set(phone,userdto);
       *
       *
       * */

    public User createUserWithPhone(String phone) {
        //创建用户
        User user=new User();
        user.setPhone(phone);
        user.setNickName(USER_NICK_NAME_PREFIX+RandomUtil.randomString(6));
        //保存用户
        save(user);
        return user;
    }
}
