/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.noaa.nws.ngitws.apps.datacleaner;

import gov.noaa.nws.ngitws.catalogs.metadata.webservice.configuration.EnableMetadataCatalogWebServiceClient;
import gov.noaa.nws.ngitws.catalogs.object.webservice.configuration.EnableObjectCatalogWebServiceClient;
import gov.noaa.nws.ngitws.configuration.application.NgitwsApplication;
import gov.noaa.nws.ngitws.configuration.rabbit.configuration.EnableExclusiveLock;
import gov.noaa.nws.ngitws.messaging.events.configuration.EnableEventsReceiver;
import gov.noaa.nws.ngitws.messaging.events.configuration.EnableEventsSender;
import org.springframework.context.annotation.Bean;
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler;

/**
 *
 * @author kevin.off
 */
@EnableMetadataCatalogWebServiceClient
@EnableObjectCatalogWebServiceClient
@EnableExclusiveLock
@EnableEventsReceiver
@EnableEventsSender
public class Application extends NgitwsApplication {
    
    public static void main(String[] args){
        Application.run(Application.class, args);
    } 

    /**
     * Task scheduler used by the @EnableScheduling and @Scheduled annotations.
     * In this case the pool size must be at least 2 so the cleaning thread
     * and the exclusive lock thread can run at the same time.
     * 
     * @return The task scheduler bean
     */
    @Bean
    public ThreadPoolTaskScheduler taskScheduler(){
        
        ThreadPoolTaskScheduler scheduler = new ThreadPoolTaskScheduler();
        scheduler.setThreadGroupName("data-cleaner-tg");
        scheduler.setThreadNamePrefix("data-cleaner-thread");
        scheduler.setPoolSize(4);
        
        return scheduler;
    }
    
}
