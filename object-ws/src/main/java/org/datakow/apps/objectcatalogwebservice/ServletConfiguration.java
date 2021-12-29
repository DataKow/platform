package org.datakow.apps.objectcatalogwebservice;

import java.io.IOException;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.Filter;
import javax.servlet.FilterChain;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpServletResponseWrapper;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.filter.OncePerRequestFilter;

/**
 *
 * @author kevin.off
 */
@Configuration
public class ServletConfiguration {

    @Bean
    public Filter setRequestAndCorrelationIdsInLogger(){
        
        OncePerRequestFilter filter = new OncePerRequestFilter() {
            
            @Override
            protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain filterChain) throws ServletException, IOException {
                
                String requestId = request.getHeader("X-Request-ID");
                String correlationId = request.getHeader("X-Correlation-ID");
                
                if (correlationId == null || correlationId.isEmpty()){
                    String theirCorrelationId = request.getHeader("X-DATAKOW-Originator-ID");
                    if (theirCorrelationId == null || theirCorrelationId.isEmpty()){
                        correlationId = UUID.randomUUID().toString();
                    }else{
                        correlationId = theirCorrelationId;
                    }
                }
                
                if (requestId == null || requestId.isEmpty()){
                    String theirRequestId = request.getHeader("X-DATAKOW-Request-ID");
                    if (theirRequestId == null || theirRequestId.isEmpty()){
                        requestId = UUID.randomUUID().toString();
                    }else{
                        requestId = theirRequestId;
                    }
                }

                ThreadContext.put("requestId", requestId);
                ThreadContext.put("correlationId", correlationId);
                
                if (!response.isCommitted()){
                    response.setHeader("X-Request-ID", requestId);
                    response.setHeader("X-Correlation-ID", correlationId);
                }
                
                Logger.getLogger(ServletConfiguration.class.getName()).log(Level.FINE, "Received request: [{0}] {1}", new Object[] {request.getMethod(), request.getServletPath()});
                
                HttpServletResponseWrapper wrapper = new HttpServletResponseWrapper(response) {
                    @Override
                    public void setStatus(int sc) {
                        super.setStatus(sc);
                        setTrackingHeaders();
                    }
                    
                    private void setTrackingHeaders(){
                        setHeader("X-Request-ID", ThreadContext.get("requestId"));
                        setHeader("X-Correlation-ID", ThreadContext.get("correlationId"));
                    }
                };
                filterChain.doFilter(request, wrapper);
                
            }
        };

        return filter;
    }
    
}