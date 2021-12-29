package org.datakow.apps.metadatacatalogwebservice;

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
public class SecurityConfiguration extends WebSecurityConfigurerAdapter{
    
    @Autowired
    AuthenticationProvider provider;
    
    
    @Override
    protected void configure(HttpSecurity http) throws Exception {
                 
            
             http
               .authenticationProvider(provider)
               .sessionManagement().sessionCreationPolicy(SessionCreationPolicy.STATELESS)
               .and()
               .authorizeRequests()
                   .antMatchers(HttpMethod.GET, "/catalogs/DATAKOW_AMDAR_METADATA/records/**").hasAnyRole("ADMIN", "AMDAR")
                   .antMatchers(HttpMethod.POST, "/catalogs/ROC_*/records/**").hasAnyRole("ADMIN", "ROC_PRODUCER")
                   .antMatchers(HttpMethod.POST, "/catalogs/DATAKOW_GAIRMET_METADATA/records/**").hasAnyRole("ADMIN", "AWC")
                   .antMatchers(HttpMethod.POST).hasRole("ADMIN")
                   .antMatchers(HttpMethod.PATCH).hasRole("ADMIN")
                   .antMatchers(HttpMethod.DELETE, "/catalogs/ROC_*/records/**").hasAnyRole("ADMIN", "ROC_PRODUCER")
                   .antMatchers(HttpMethod.DELETE, "/catalogs/DATAKOW_GAIRMET_METADATA/records/**").hasAnyRole("ADMIN", "AWC")
                   .antMatchers(HttpMethod.DELETE).hasRole("ADMIN")
                   .antMatchers(HttpMethod.PUT, "/catalogs/ROC_*/records/**").hasAnyRole("ADMIN", "ROC_PRODUCER")
                   .antMatchers(HttpMethod.PUT, "/catalogs/DATAKOW_GAIRMET_METADATA/records/**").hasAnyRole("ADMIN", "AWC")
                   .antMatchers(HttpMethod.PUT).hasRole("ADMIN")
                     
                   .antMatchers(HttpMethod.GET, "/metaws/v1/catalogs/DATAKOW_AMDAR_METADATA/records/**").hasAnyRole("ADMIN", "AMDAR")
                   .antMatchers(HttpMethod.POST, "/metaws/v1/catalogs/ROC_*/records/**").hasAnyRole("ADMIN", "ROC_PRODUCER")
                   .antMatchers(HttpMethod.POST, "/metaws/v1/catalogs/DATAKOW_GAIRMET_METADATA/records/**").hasAnyRole("ADMIN", "AWC")
                   .antMatchers(HttpMethod.DELETE, "/metaws/v1/catalogs/ROC_*/records/**").hasAnyRole("ADMIN", "ROC_PRODUCER")
                   .antMatchers(HttpMethod.DELETE, "/metaws/v1/catalogs/DATAKOW_GAIRMET_METADATA/records/**").hasAnyRole("ADMIN", "AWC")
                   .antMatchers(HttpMethod.PUT, "/metaws/v1/catalogs/ROC_*/records/**").hasAnyRole("ADMIN", "ROC_PRODUCER")
                   .antMatchers(HttpMethod.PUT, "/metaws/v1/catalogs/DATAKOW_GAIRMET_METADATA/records/**").hasAnyRole("ADMIN", "AWC")
                   .anyRequest().permitAll()
               .and()
                   .csrf().disable()
               .httpBasic();
    }

}
