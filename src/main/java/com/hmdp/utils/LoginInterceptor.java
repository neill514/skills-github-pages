package com.hmdp.utils;

import cn.hutool.core.bean.BeanUtil;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.User;
import io.netty.util.internal.StringUtil;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.servlet.HandlerInterceptor;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class LoginInterceptor implements HandlerInterceptor {
         public boolean preHandle(final HttpServletRequest request, final HttpServletResponse response, final Object handler) throws Exception {

             //1.判断是否需要拦截（ThreadLocal中是否有用户)
             if(UserHolder.getUser()==null){
                 response.setStatus(401);
                 return false;
             }
             return true;
         }

         public void afterCompletion(final HttpServletRequest request, final HttpServletResponse response, final Object handler, final Exception ex) throws Exception {
           //移除用户
             UserHolder.removeUser();
         }
}
