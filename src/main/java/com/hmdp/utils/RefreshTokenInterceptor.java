package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.UserDTO;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class RefreshTokenInterceptor implements HandlerInterceptor {
    private StringRedisTemplate stringRedisTemplate;

    public RefreshTokenInterceptor(StringRedisTemplate stringRedisTemplate){
        this.stringRedisTemplate=stringRedisTemplate;
    }
         public boolean preHandle(final HttpServletRequest request, final HttpServletResponse response, final Object handler) throws Exception {

            //获取session
            /* HttpSession session = request.getSession();*/
             //获取请求头中的token
            String token=request.getHeader("authorization");
            if(token==null){
                return true;
            }
             // 基于TOKEN获取redis中的用户
             String key=RedisConstants.LOGIN_USER_KEY + token;
             Map<Object, Object> userMap = stringRedisTemplate.opsForHash().entries(key);
             //获取session中的用户
             /*Object user = session.getAttribute("user");*/

             //判断用户是否存在
             if (userMap.isEmpty()) {
                 //map返回空为true
                 return true;
             }
             // 将查询到的Hash数据转为UserDTO对象
             UserDTO userDTO = BeanUtil.fillBeanWithMap(userMap, new UserDTO(), false);
             //存在，保存用户信息到ThreadLocal
             UserHolder.saveUser( userDTO);
             // 刷新token有效期
             stringRedisTemplate.expire(key,RedisConstants.LOGIN_USER_TTL, TimeUnit.MINUTES);
             //放行
             return true;
         }

         public void afterCompletion(final HttpServletRequest request, final HttpServletResponse response, final Object handler, final Exception ex) throws Exception {
           //移除用户
             UserHolder.removeUser();
         }
}
