package org.datakow.apps.objectcatalogwebservice;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Configuration;
import org.springframework.http.HttpMethod;
import org.springframework.security.authentication.AuthenticationProvider;
import org.springframework.security.config.annotation.web.builders.HttpSecurity;
import org.springframework.security.config.annotation.web.configuration.EnableWebSecurity;
import org.springframework.security.config.annotation.web.configuration.WebSecurityConfigurerAdapter;
import org.springframework.security.config.http.SessionCreationPolicy;

/**
 *
 * @author kevin.off
 */
@Configuration
@EnableWebSecurity
public class SecurityConfiguration extends WebSecurityConfigurerAdapter {
    
    @Autowired
    AuthenticationProvider provider;
    
    
    @Override
    protected void configure(HttpSecurity http) throws Exception {
            http
                .authenticationProvider(provider)
                .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
                .and()
                .authorizeRequests()
                    .antMatchers(HttpMethod.GET).permitAll() //even anonymous
                    .antMatchers(HttpMethod.HEAD).permitAll()
                    .anyRequest().authenticated() //must have a user account so we can check for authorization
                .and()
                    .csrf().disable()
                .httpBasic();
    }

    
    
}
