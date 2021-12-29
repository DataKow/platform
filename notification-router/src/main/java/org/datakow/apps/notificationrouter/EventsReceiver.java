package org.datakow.apps.notificationrouter;


import org.datakow.messaging.events.CatalogEventsReceiver;
import org.datakow.messaging.events.events.CatalogEvent;
import org.datakow.messaging.events.events.Event;
import org.datakow.messaging.events.events.EventType;
import org.datakow.messaging.events.events.RecordAssociationEvent;
import org.datakow.messaging.events.events.RecordEvent;
import org.datakow.messaging.events.events.SubscriptionEvent;

import org.datakow.catalogs.metadata.CatalogRegistry;
import org.springframework.beans.factory.annotation.Autowired;
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
    private SubscriptionRegistry subscriptionRegistry;
    
    @Autowired
    private NotificationRouter notificationRouter;
    
    @Override
    public void receiveEvent(Event event) {
        
        switch(event.getEventType()){
            case EventType.CATALOG_RECORD:
                notificationRouter.receiveRecordEvent((RecordEvent)event);
                break;
            case EventType.CATALOG_RECORD_ASSOCIATION:
                notificationRouter.receiveRecordAssociationEvent((RecordAssociationEvent)event);
                break;
            case EventType.SUBSCRIPTION:
                subscriptionRegistry.receiveSubscriptionUpdate((SubscriptionEvent)event);
                break;
            case EventType.CATALOG:
                catalogRegistry.expireCache(((CatalogEvent)event).getCatalogIdentifier());
                break;
        }
        
    }
    
}
