package org.datakow.apps.notificationrouter;

import org.datakow.messaging.events.CatalogEventsReceiverClient;
import org.datakow.messaging.events.events.EventType;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 *
 * @author kevin.off
 */
@Component
public class Init implements CommandLineRunner{

    @Autowired
    private SubscriptionRegistry subscriptionRegistry;
    
    @Autowired
    private CatalogEventsReceiverClient eventsReceiverClient;
    
    @Override
    public void run(String ... args){
      
        try {
            subscriptionRegistry.initializeSubscriptions();
        } catch (IOException ex) {
            Logger.getLogger(Init.class.getName()).log(Level.SEVERE, null, ex);
        }
        
        eventsReceiverClient.startReceivingEventsShared(EventType.CATALOG_RECORD, EventType.CATALOG_RECORD_ASSOCIATION);
        LogManager.getLogger(this.getClass()).info("Started listening to record events on a shared queue");
        
        eventsReceiverClient.startReceivingEventsIndividual(EventType.CATALOG, EventType.SUBSCRIPTION);
        LogManager.getLogger(this.getClass()).info("Started listening to catalog events and subscription updates on an individual queue");
    }
    
}
