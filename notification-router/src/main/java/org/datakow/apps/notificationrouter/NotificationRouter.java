package org.datakow.apps.notificationrouter;


import com.fasterxml.jackson.core.JsonProcessingException;

import org.datakow.catalogs.metadata.database.MetadataDataCoherence;
import org.datakow.catalogs.metadata.webservice.MetadataCatalogWebserviceClient;
import org.datakow.catalogs.subscription.QueryStringSubscription;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.DotNotationMap;
import org.datakow.messaging.events.events.EventAction;
import org.datakow.messaging.events.events.RecordAssociationEvent;
import org.datakow.messaging.events.events.RecordEvent;
import org.datakow.messaging.notification.NotificationSenderClient;
import org.datakow.messaging.notification.notifications.Notification;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datakow.catalogs.metadata.Catalog;
import org.datakow.catalogs.metadata.CatalogRegistry;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

/**
 *
 * @author kevin.off
 */
@Component
public class NotificationRouter {
        
    @Autowired
    private NotificationSenderClient notificationClient;
    
    @Autowired
    private SubscriptionRegistry subscriptionRegistry;
    
    @Autowired
    private CatalogRegistry catalogRegistry;
    
    @Autowired
    private MetadataCatalogWebserviceClient metadataWebserviceClient;
    
    private final Logger logger = LogManager.getLogger(this.getClass());

    public void receiveRecordEvent(RecordEvent event) {
        
        if (event.getEventAction().equalsIgnoreCase(EventAction.CREATED)){
            if (event.getCatalogIdentity() != null){
                
                Notification notification = null;
                
                CatalogIdentity metadCatalogIdentity = null;
                String catalogIdentifier = event.getCatalogIdentity().getCatalogIdentifier();
                
                Catalog catalog;
                try{
                     catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
                }catch(Exception e){
                    logger.error("Error wile finding catalog: " + catalogIdentifier + " from the metadata catalog web service", e);
                    return;
                }
                
                if (catalog != null){
                    if (catalog.getCatalogType().equalsIgnoreCase("metadata")){
                        notification = new Notification();
                        notification.setObjectMetadataIdentity(event.getCatalogIdentity());
                        metadCatalogIdentity = event.getCatalogIdentity();
                    }else if (catalog.getCatalogType().equalsIgnoreCase("object")){
                        notification = new Notification();
                        notification.setObjectIdentity(event.getCatalogIdentity());
                    }
                    
                    if (notification != null){
                        notification.setNotificationId(UUID.randomUUID().toString());
                        List<String> recipients = getInterestedSubscriptionQueueNames(
                                Arrays.asList(catalogIdentifier), 
                                metadCatalogIdentity,
                                "created");
                        if (!recipients.isEmpty()){
                            logger.debug("Sending catalog record creation notification to: " + recipients.stream().collect(Collectors.joining(", ")));
                            notificationClient.sendNotification(notification, recipients);
                        }
                    }
                }else{
                    logger.error("Could not find catalog: " + catalogIdentifier + " in the metadata catalog web service. No action will be taken on event");
                }
            }
        }
    }
    
    public void receiveRecordAssociationEvent(RecordAssociationEvent event){
        if (event.getEventAction().equalsIgnoreCase(EventAction.CREATED)){
            if (event.getObjectIdentity() != null && event.getObjectMetadataIdentity() != null){
                
                CatalogIdentity metadataCatalogIdentity = event.getObjectMetadataIdentity();
                String metadataCatalogIdentifier = event.getObjectMetadataIdentity().getCatalogIdentifier();
                String objectCatalogIdentifier = event.getObjectIdentity().getCatalogIdentifier();
                
                Catalog catalog;
                try{
                     catalog = catalogRegistry.getByCatalogIdentifier(metadataCatalogIdentifier);
                }catch(Exception e){
                    logger.error("Error wile finding catalog: " + metadataCatalogIdentifier + " from the metadata catalog web service", e);
                    return;
                }
                
                if (catalog != null){
                    
                    List<String> recipients = getInterestedSubscriptionQueueNames(
                            Arrays.asList(metadataCatalogIdentifier, objectCatalogIdentifier), 
                            metadataCatalogIdentity,
                            "associated");
                    if (!recipients.isEmpty()){
                        Notification notification = new Notification();
                        notification.setNotificationId(UUID.randomUUID().toString());
                        notification.setObjectIdentity(event.getObjectIdentity());
                        notification.setObjectMetadataIdentity(event.getObjectMetadataIdentity());
                        logger.debug("Sending catalog record association notification to: " + recipients.stream().collect(Collectors.joining(", ")));
                        notificationClient.sendNotification(notification, recipients);
                    }
                }
            }
        }
    }
    
    private List<String> getInterestedSubscriptionQueueNames(List<String> catalogIdentifiers, 
            CatalogIdentity metadataCatalogIdentity, String action){
        List<QueryStringSubscription> interested = new ArrayList<>();
        MetadataCatalogRecord record = null;
        DotNotationMap metadata = null;
        for(QueryStringSubscription sub : subscriptionRegistry.getAll()){
            if (meetsCatalogIdentifierCriteria(sub, catalogIdentifiers)){
                if (meetsCatalogActionCriteria(sub, action)){
                    if (metadataCatalogIdentity != null){
                        if (record == null){
                            try{
                                record = getRecord(metadataCatalogIdentity);
                                if (record != null){
                                    metadata = record.getDocument();
                                }else{
                                    logger.error("Received a 404 when retrieving record " + 
                                            metadataCatalogIdentity.getCatalogIdentifier() + " : " + 
                                            metadataCatalogIdentity.getRecordIdentifier());
                                    return new ArrayList<>();
                                }
                            }catch(JsonProcessingException ex){
                                logger.error("There was an error parsing the response from json when getting record " + 
                                        metadataCatalogIdentity.getCatalogIdentifier() + " : " + 
                                        metadataCatalogIdentity.getRecordIdentifier(), 
                                        ex);
                                
                                return new ArrayList<>();
                            }
                        }
                    }
                    if (sub.meetsCriteria(metadata)){
                        interested.add(sub);
                    }
                }
            }
        }
        
        String interestedString = interested.stream().map((s)->s.getId()).collect(Collectors.joining(", "));
        logger.debug("Found " + interested.size() + " interested subscribers: " + interestedString);
        
        return interested.stream().map((sub)->sub.getQueueName()).collect(Collectors.toList());

    }
    
    private boolean meetsCatalogIdentifierCriteria(QueryStringSubscription subscription, List<String> targetCatalogs){

        if (subscription.getCatalogIdentifier() != null && !subscription.getCatalogIdentifier().isEmpty()){
            return targetCatalogs.contains(subscription.getCatalogIdentifier());
        }else{
            return true;
        }
    }
    
    private boolean meetsCatalogActionCriteria(QueryStringSubscription subscription, String catalogAction){
        if (subscription.getCatalogAction() == null){
            return catalogAction.equalsIgnoreCase("associated");
        }else{
            return subscription.getCatalogAction().equalsIgnoreCase(catalogAction);
        }
    }
    
    @Retryable(maxAttempts = 3, backoff = @Backoff(delay = 100))
    private MetadataCatalogRecord getRecord(CatalogIdentity metadataCatalogIdentity) throws JsonProcessingException{
        MetadataCatalogRecord record = metadataWebserviceClient.getById(
                                        metadataCatalogIdentity.getCatalogIdentifier(), 
                                        metadataCatalogIdentity.getRecordIdentifier(),
                                        null,
                                        MetadataDataCoherence.CONSISTENT);
        if (record == null){
            throw new IllegalStateException("Record " + metadataCatalogIdentity.getCatalogIdentifier() + ":" + metadataCatalogIdentity.getRecordIdentifier() + " could not be found.");
        }
        return record;
    }
    
}
