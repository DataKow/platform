package org.datakow.apps.subscriptionwebservice;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import springfox.documentation.spi.DocumentationType;
import springfox.documentation.spring.web.plugins.Docket;

/**
 *
 * @author kevin.off
 */
@Configuration
public class SwaggerConfig {
    
    @Bean
    public Docket swaggerSpringMvcPlugin(){
        
        return new Docket(DocumentationType.SWAGGER_2)
                .select()
                .paths((t) -> {
                    return t.startsWith("/subws/v1");
                }).build();
        
    }
    
}
