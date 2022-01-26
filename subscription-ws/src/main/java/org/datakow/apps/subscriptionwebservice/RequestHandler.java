package org.datakow.apps.subscriptionwebservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.apps.subscriptionwebservice.exception.RecordNotFoundException;
import org.datakow.apps.subscriptionwebservice.exception.UnauthorizedException;
import org.datakow.catalogs.metadata.database.MetadataDataCoherence;
import org.datakow.catalogs.metadata.webservice.MetadataCatalogWebserviceClient;
import org.datakow.catalogs.metadata.webservice.MetadataCatalogWebserviceRequest;
import org.datakow.catalogs.subscription.QueryStringSubscription;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.CloseableIterator;
import org.datakow.core.components.DotNotationMap;
import org.datakow.messaging.events.CatalogEventsSenderClient;
import org.datakow.messaging.events.events.EventAction;
import org.datakow.messaging.events.events.SubscriptionAction;
import org.datakow.messaging.events.events.SubscriptionEvent;
import org.datakow.messaging.notification.NotificationReceiverClient;
import org.datakow.security.access.AccessManager;
import io.swagger.annotations.ApiOperation;
import io.swagger.annotations.ApiParam;
import io.swagger.annotations.ApiResponse;
import io.swagger.annotations.ApiResponses;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.servlet.http.HttpServletResponse;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.ThreadContext;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import springfox.documentation.annotations.ApiIgnore;

/**
 *
 * @author kevin.off
 */
@RestController
@CrossOrigin
@RequestMapping(value = {"/"})
public class RequestHandler {

    @Autowired
    CatalogEventsSenderClient eventSender;
    
    @Autowired
    NotificationReceiverClient rabbitClient;
    
    @Autowired
    MetadataCatalogWebserviceClient client;
    
    @Autowired
    AccessManager accessManager;
    
    @RequestMapping(value="/", method = {RequestMethod.GET, RequestMethod.HEAD}, produces = {MediaType.TEXT_PLAIN_VALUE})
    public ResponseEntity defaultPage(){
        return new ResponseEntity(HttpStatus.OK);
    }
    
    @RequestMapping(value = "/subscriptions", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<QueryStringSubscription>> getByQuery(
            @ApiParam(value = "A FIQL query used to find subscriptions") 
            @RequestParam(value = "s", required = false, defaultValue = "") String fiql,
            @ApiParam(value = "The maximum number of subscriptions to return") 
            @RequestParam(value = "limit", required = false, defaultValue = "-1") int limit) throws JsonProcessingException, IOException{
        
        ThreadContext.put("catalogIdentifier", "subscriptions");
        LogManager.getLogger(this.getClass()).info("Returning Subscriptions from query: " + fiql);
        
        ThreadContext.put("catalogIdentifier", "subscriptions");
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        List<QueryStringSubscription> subs = getByQuery(auth, fiql);
        return new ResponseEntity<>(subs, HttpStatus.OK);
    }
    
    @RequestMapping(value = "/subscriptions/{id:.+}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity getById(
            @ApiParam(value = "The Subscription Id") 
            @PathVariable("id") String id, HttpServletResponse response) throws JsonProcessingException, IOException{
        
        ThreadContext.put("catalogIdentifier", "subscriptions");
        ThreadContext.put("subscriptionId", id);
        
        LogManager.getLogger(this.getClass()).info("Returning Subscription " + id);
        
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        
        List<MetadataCatalogRecord> records = client.getByQueryAndStream(
                "subscriptions", 
                MetadataCatalogWebserviceRequest.builder()
                        .withQuery("Doc.id==" + id)
                        .withDataCoherence(MetadataDataCoherence.CONSISTENT))
                .toList();
        
        if (records != null && !records.isEmpty()){
            if (records.size() > 1){
                LogManager.getLogger().error("There are more than 1 subscriptions with the id " + id);
                return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
            }else{
                MetadataCatalogRecord record = records.get(0);
                QueryStringSubscription sub = QueryStringSubscription.fromJson(record.getDocument().toJson());
                if(accessManager.canRead(auth, record.getStorage().getRealm(), sub.getUserName())){
                    return new ResponseEntity<>(sub, HttpStatus.OK);
                }else{
                    throw new UnauthorizedException(record.getStorage().getRealm());
                }
            }
        }else{
            throw new RecordNotFoundException(new CatalogIdentity("subscriptions", id));
        }
    }
    
    @RequestMapping(value = "/users/{username:.+}/subscriptions", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<QueryStringSubscription>> getByUsername(
            @ApiParam(value = "The username to filter by") 
            @PathVariable("username") String username) throws JsonProcessingException, IOException{
        
        ThreadContext.put("catalogIdentifier", "subscriptions");
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        LogManager.getLogger(this.getClass()).info("Returning Subscriptions for user " + username);
        List<QueryStringSubscription> subs = getByQuery(auth, "Doc.userName==" + username);
        return new ResponseEntity<>(subs, HttpStatus.OK);
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier:.+}/subscriptions", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<QueryStringSubscription>> getByCatalogIdentifier(
            @ApiParam(value = "The catalog identififer to filter on") 
            @PathVariable("catalogIdentifier") String catalogIdentifier) throws JsonProcessingException, IOException{
        
        ThreadContext.put("catalogIdentifier", "subscriptions");
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        LogManager.getLogger(this.getClass()).info("Returning Subscriptions for catalogIdentifier " + catalogIdentifier);
        List<QueryStringSubscription> subs = getByQuery(auth, "Doc.catalogIdentifier==" + catalogIdentifier);
        return new ResponseEntity<>(subs, HttpStatus.OK);
    }
    
    @RequestMapping(value = "/endpoints/{endpointIdentifier:.+}/subscriptions", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<QueryStringSubscription>> getByEndpointIdentifier(
            @ApiParam(value = "The endpoint identifier to filter on") 
            @PathVariable("endpointIdentifier") String endpointIdentifier) throws JsonProcessingException, IOException{
        
        ThreadContext.put("catalogIdentifier", "subscriptions");
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        LogManager.getLogger(this.getClass()).info("Returning Subscriptions for endpointIdentifier " + endpointIdentifier);
        List<QueryStringSubscription> subs = getByQuery(auth, "Doc.endpointIdentifier==" + endpointIdentifier);
        return new ResponseEntity<>(subs, HttpStatus.OK);
    }
    
    @ApiResponses({
        @ApiResponse(code = 201, response = QueryStringSubscription.class, message = "Created"),
        @ApiResponse(code = 401, response = String.class, message = "Not Authenticated"),
        @ApiResponse(code = 403, response = String.class, message = "Not Authorized"),
        @ApiResponse(code = 404, response = String.class, message = "Not Found"),
        @ApiResponse(code = 500, response = String.class, message = "Internal Server Error")
    })
    @ApiOperation(value = "Creates and registers a subscription", code = 201, response = QueryStringSubscription.class)
    @RequestMapping(value = "/subscriptions", method = {RequestMethod.POST, RequestMethod.PUT}, produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity subscribe(
            @ApiParam(value = "The Subscription Id") 
            @RequestParam(value = "id", required = false, defaultValue = "") String id,
            @ApiParam(value = "The identifier of the endpoint intended to service this subscription") 
            @RequestParam(value = "endpointIdentifier", required = false, defaultValue = "AMQP") String endpointIdentifier,
            @ApiParam(value = "The catalog that the subscription is targeting") 
            @RequestParam(value = "catalogIdentifier", required = false, defaultValue = "") String catalogIdentifier,
            @ApiParam(value = "The action that the subscription is interested in", example = "created|associated|updated|deleted") 
            @RequestParam(value = "catalogAction", required = false, defaultValue = "associated") String catalogAction,
            @ApiParam(value = "The query that will be used to compare the record with") 
            @RequestParam(value = "s", required = false, defaultValue = "") String fiql,
            @ApiIgnore 
            @RequestParam Map<String, String> allParams) throws JsonProcessingException, IOException {
        
        ThreadContext.put("catalogIdentifier", "subscriptions");
        
        id = (id.isEmpty() ? null : id);
        
        if (id == null || id.isEmpty()){
            id = UUID.randomUUID().toString();
            LogManager.getLogger(this.getClass()).info("The SubscriptionId was not specified so one was generated: " + id);
        }
        ThreadContext.put("subscriptionId", id);
        catalogIdentifier = (catalogIdentifier.isEmpty() ? null : catalogIdentifier);
        fiql = (fiql.isEmpty() ? null : fiql);
        
        String username = SecurityContextHolder.getContext().getAuthentication().getName();

        LogManager.getLogger(this.getClass()).info("Received SUBSCRIBE request. "
                + "Id: " + id + ", "
                + "endpointIdentifier: " + endpointIdentifier + ", "
                + "catalogIdentifier: " + catalogAction + ", "
                + "catalogAction: " + catalogAction + ", "
                + "query: " + fiql);

        List<String> params = Arrays.asList(new String[]{
            "id", "endpointIdentifier", "catalogIdentifier", "catalogAction", "s", "createQueue"
        });
        Map<String, String> additionalParams = new HashMap<>();
        for(Map.Entry<String, String> entry : allParams.entrySet()){
            if (!params.contains(entry.getKey())){
                additionalParams.put(entry.getKey(), entry.getValue());
            }
        }

        LogManager.getLogger(this.getClass()).info("About to create subscription from request for subscription: " + id);
        QueryStringSubscription subscription = new QueryStringSubscription(id, fiql, catalogIdentifier, catalogAction, username, endpointIdentifier);
        subscription.setProperties(additionalParams);
        LogManager.getLogger(this.getClass()).info("Created subscription from request for subscription: " + subscription.getId());

        LogManager.getLogger(this.getClass()).info("About to save subscription to database");
        HttpStatus status = saveSubscription(subscription);
        
        if (status != HttpStatus.CREATED && status != HttpStatus.OK){
            throw new IllegalStateException("The response from saving the subscription was " + status.toString());
        }
        LogManager.getLogger(this.getClass()).info("Saved subscription to database");
        
        String eventAction = status == HttpStatus.CREATED ? EventAction.CREATED : EventAction.UPDATED;
        
        LogManager.getLogger(this.getClass()).info("About to create and bind queue for subscription: " + subscription.getId());
        rabbitClient.createAndBindQueue(subscription.getId());
        LogManager.getLogger(this.getClass()).info("Created and bound queue for subscription: " + subscription.getId());

        LogManager.getLogger(this.getClass()).info("About to send SUBSCRIBE event for subscription: " + subscription.getId());
        sendEvent(subscription.getId(), SubscriptionAction.SUBSCRIBE, eventAction, endpointIdentifier);
        LogManager.getLogger(this.getClass()).info("Sent SUBSCRIBE event for subscription: " + subscription.getId());
        
        
        return new ResponseEntity<>(subscription, status);
    }
    
    @RequestMapping(value = "/subscriptions/{id:.+}", method = RequestMethod.DELETE)
    public ResponseEntity unsubscribe(
            @ApiParam(value = "The Subscription Id") 
            @PathVariable("id") String id) throws JsonProcessingException, IOException{
        
        ThreadContext.put("catalogIdentifier", "subscriptions");
        ThreadContext.put("subscriptionId", id);
        
        LogManager.getLogger(this.getClass()).info("Received an UNSUBSCRIBE request for subscription: " + id);

        LogManager.getLogger(this.getClass()).info("Getting subscription " + id + " from the database.");
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        
        List<MetadataCatalogRecord> records = client.getByQueryAndStream(
                "subscriptions", 
                MetadataCatalogWebserviceRequest.builder()
                        .withQuery("Doc.id==" + id)
                        .withDataCoherence(MetadataDataCoherence.CONSISTENT))
                .toList();
        QueryStringSubscription sub = null;
        
        if (records != null && !records.isEmpty()){
            if (records.size() > 1){
                LogManager.getLogger().error("There are more than 1 subscriptions with the id " + id);
                return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
            }else{
                MetadataCatalogRecord record = records.get(0);
                sub = QueryStringSubscription.fromJson(record.getDocument().toJson());
                if(!accessManager.canWrite(auth, record.getStorage().getRealm(), record.getStorage().getPublisher())){
                    throw new UnauthorizedException(record.getStorage().getRealm());
                }
            }
        }
        
        if (sub != null){
            LogManager.getLogger(this.getClass()).info("About to delete subscription " + id + " from the database.");
            deleteSubscription(id);
            LogManager.getLogger(this.getClass()).info("Deleted subscription " + id + " from the database.");

            LogManager.getLogger(this.getClass()).info("About to delete the queue for subscription " + id);
            rabbitClient.deleteQueue(id);
            LogManager.getLogger(this.getClass()).info("Deleted the queue for subscription " + id);

            LogManager.getLogger(this.getClass()).info("About to send UNSUBSCRIBE event for subscription: " + id);
            sendEvent(id, SubscriptionAction.UNSUBSCRIBE, EventAction.DELETED, sub.getEndpointIdentifier());
            LogManager.getLogger(this.getClass()).info("Sent UNSUBSCRIBE event for subscription: " + id);
            return new ResponseEntity(HttpStatus.OK);
        }else{
            LogManager.getLogger(this.getClass()).warn("Unable to find subscription " + id + " in the database for the delete request");
            return new ResponseEntity(HttpStatus.OK);
        }
        
    }

    @RequestMapping(value = "/subscriptions/{id}/pause", method = RequestMethod.POST)
    public ResponseEntity pause(
            @ApiParam(value = "The Subscription Id") 
            @PathVariable("id") String id) throws JsonProcessingException, IOException{
        
        ThreadContext.put("catalogIdentifier", "subscriptions");
        ThreadContext.put("subscriptionId", id);
        
        LogManager.getLogger(this.getClass()).info("Received a PAUSE request for subscription: " + id);

        LogManager.getLogger(this.getClass()).info("Getting subscription " + id + " from the database.");
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        
        List<MetadataCatalogRecord> records = client.getByQueryAndStream(
                "subscriptions", 
                MetadataCatalogWebserviceRequest.builder()
                        .withQuery("Doc.id==" + id)
                        .withDataCoherence(MetadataDataCoherence.CONSISTENT))
                .toList();
        QueryStringSubscription sub = null;
        
        if (records != null && !records.isEmpty()){
            if (records.size() > 1){
                LogManager.getLogger().error("There are more than 1 subscriptions with the id " + id);
                return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
            }else{
                MetadataCatalogRecord record = records.get(0);
                sub = QueryStringSubscription.fromJson(record.getDocument().toJson());
                if(!accessManager.canWrite(auth, record.getStorage().getRealm(), record.getStorage().getPublisher())){
                    throw new UnauthorizedException(record.getStorage().getRealm());
                }
            }
        }
        if (sub != null){
            LogManager.getLogger(this.getClass()).info("Subscription " + id + " was found in the database");
            sub.setPaused(true);
            LogManager.getLogger(this.getClass()).info("About to save the paused subscription in the database: " + id);
            saveSubscription(sub);
            LogManager.getLogger(this.getClass()).info("Saved the paused subscription in the database: " + id);
            
            LogManager.getLogger(this.getClass()).info("About to send the PAUSE event for subscription: " + id);
            sendEvent(id, SubscriptionAction.PAUSE, EventAction.UPDATED, sub.getEndpointIdentifier());
            LogManager.getLogger(this.getClass()).info("Sent the PAUSE event for subscription: " + id);
            
            return new ResponseEntity(HttpStatus.OK);
        }else{
            throw new RecordNotFoundException(new CatalogIdentity("subscriptions", id));
        }

    }
    
    @RequestMapping(value = "/subscriptions/{id}/resume", method = RequestMethod.POST)
    public ResponseEntity resume(
            @ApiParam(value = "The Subscription Id") 
            @PathVariable("id") String id, 
            HttpServletResponse response) throws JsonProcessingException, IOException{
        
        ThreadContext.put("catalogIdentifier", "subscriptions");
        ThreadContext.put("subscriptionId", id);
        
        LogManager.getLogger(this.getClass()).info("Received a PAUSE request for subscription: " + id);

        LogManager.getLogger(this.getClass()).info("Getting subscription " + id + " from the database.");
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        
        List<MetadataCatalogRecord> records = client.getByQueryAndStream(
                "subscriptions", 
                MetadataCatalogWebserviceRequest.builder()
                        .withQuery("Doc.id==" + id)
                        .withDataCoherence(MetadataDataCoherence.CONSISTENT))
                .toList();
        QueryStringSubscription sub = null;
        
        if (records != null && !records.isEmpty()){
            if (records.size() > 1){
                LogManager.getLogger().error("There are more than 1 subscriptions with the id " + id);
                return new ResponseEntity(HttpStatus.INTERNAL_SERVER_ERROR);
            }else{
                MetadataCatalogRecord record = records.get(0);
                sub = QueryStringSubscription.fromJson(record.getDocument().toJson());
                if(!accessManager.canWrite(auth, record.getStorage().getRealm(), record.getStorage().getPublisher())){
                    throw new UnauthorizedException(record.getStorage().getRealm());
                }
            }
        }
        if (sub != null){
            LogManager.getLogger(this.getClass()).info("Subscription " + id + " was found in the database");
            sub.setPaused(false);
            LogManager.getLogger(this.getClass()).info("About to save the resumed subscription in the database: " + id);
            saveSubscription(sub);
            LogManager.getLogger(this.getClass()).info("Saved the resumed subscription in the database: " + id);
            
            LogManager.getLogger(this.getClass()).info("About to send the RESUME event for subscription: " + id);
            sendEvent(id, SubscriptionAction.RESUME, EventAction.UPDATED, sub.getEndpointIdentifier());
            LogManager.getLogger(this.getClass()).info("Sent the RESUME event for subscription: " + id);
            return new ResponseEntity(HttpStatus.OK);
        }else{
            LogManager.getLogger(this.getClass()).info("Unable to find subscription " + id + " in the database for the resume request.");
            throw new RecordNotFoundException(new CatalogIdentity("subscriptions", id));
        }

    }
    
    private HttpStatus saveSubscription(QueryStringSubscription subscription) throws JsonProcessingException, IOException{
        
        List<MetadataCatalogRecord> records = client.getByQueryAndStream(
                "subscriptions", 
                MetadataCatalogWebserviceRequest.builder()
                        .withQuery("Doc.id==" + subscription.getId())
                        .withDataCoherence(MetadataDataCoherence.CONSISTENT))
                .toList();
        if (records.size() == 1){
            if(!accessManager.canWrite(SecurityContextHolder.getContext().getAuthentication(), records.get(0).getStorage().getRealm(), records.get(0).getStorage().getPublisher())){
                throw new UnauthorizedException(records.get(0).getStorage().getRealm());
            }
        }else if (records.size() > 1){
            throw new IllegalStateException("There is more than 1 subscription with the id " + subscription.getId());
        }
        
        if (records.isEmpty()){
            client.create("subscriptions", subscription.getId(), "internal", null, DotNotationMap.fromJson(subscription.toJson()), null);
            return HttpStatus.CREATED;
        }else{
            boolean result = client.updateOneByQuery("subscriptions", "Doc.id==" + subscription.getId(), null, "internal", null, DotNotationMap.fromJson(subscription.toJson()), null);
            if (result){
                return HttpStatus.OK;
            }else{
                throw new RuntimeException("There was an error updating your subscription: " + subscription.getId());
            }
        }
        
    }
    
    private void sendEvent(String subscriptionId, String subscriptionAction, String eventAction, String endpointIdentifier){
        SubscriptionEvent event = new SubscriptionEvent(subscriptionId, subscriptionAction, eventAction, endpointIdentifier);
        eventSender.sendEvent(event, subscriptionAction, subscriptionId);
    }
    
    private void deleteSubscription(String subscriptionId) throws JsonProcessingException{
        client.deleteByQuery("subscriptions", "Doc.id==" + subscriptionId);
    }
    
    private List<QueryStringSubscription> getByQuery(Authentication auth, String fiql) throws JsonProcessingException, IOException{
        List<QueryStringSubscription> subs = new ArrayList<>();
        
        CloseableIterator<MetadataCatalogRecord> records = client.getByQueryAndStream("subscriptions", MetadataCatalogWebserviceRequest.builder().withQuery(fiql).withDataCoherence(MetadataDataCoherence.CONSISTENT));
        while(records.hasNext()){
            MetadataCatalogRecord record = records.next();
            QueryStringSubscription sub = QueryStringSubscription.fromJson(record.getDocument().toJson());
            if (accessManager.canRead(auth, record.getStorage().getRealm(), sub.getUserName())){
                subs.add(sub);
            }
        }
        return subs;
    }
    
    
    
    
}
