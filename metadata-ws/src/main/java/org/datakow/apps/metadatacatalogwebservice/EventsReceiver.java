package org.datakow.apps.metadatacatalogwebservice;


import org.datakow.messaging.events.CatalogEventsReceiver;
import org.datakow.messaging.events.CatalogEventsReceiverClient;
import org.datakow.messaging.events.events.CatalogEvent;
import org.datakow.messaging.events.events.Event;
import org.datakow.messaging.events.events.EventType;

import org.datakow.catalogs.metadata.CatalogRegistry;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

/**
 *
 * @author kevin.off
 */
@Component
public class EventsReceiver implements CatalogEventsReceiver{

    @Autowired
    private CatalogRegistry catalogRegistry;
    
    @Autowired
    private CatalogEventsReceiverClient client;
    
    @EventListener({ContextRefreshedEvent.class})
    void startListening(){
        client.startReceivingEventsIndividual(EventType.CATALOG);
    }
    
    @Override
    public void receiveEvent(Event event) {
        
        switch(event.getEventType()){
            case EventType.CATALOG:
                catalogRegistry.expireCache(((CatalogEvent)event).getCatalogIdentifier());
                break;
        }
        
    }
    
}
