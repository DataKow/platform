package org.datakow.apps.notificationrouter;


import org.datakow.catalogs.subscription.QueryStringSubscription;
import org.datakow.catalogs.subscription.webservice.SubscriptionWebserviceClient;
import org.datakow.messaging.events.events.SubscriptionAction;
import org.datakow.messaging.events.events.SubscriptionEvent;
import java.io.IOException;
import java.net.ConnectException;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.logging.Level;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.retry.annotation.Backoff;
import org.springframework.retry.annotation.Retryable;
import org.springframework.stereotype.Component;

/**
 *
 * @author kevin.off
 */
@Component
public class SubscriptionRegistry {
    
    private final Map<String, QueryStringSubscription> subscriptions = new ConcurrentHashMap<>();
    
    @Autowired
    SubscriptionWebserviceClient client;
    
    public void add(QueryStringSubscription sub){
        this.subscriptions.put(sub.getId(), sub);
    }
    
    public void remove(String subscriptionId){
        this.subscriptions.remove(subscriptionId);
    }
    
    public Collection<QueryStringSubscription> getAll(){
        return subscriptions.values();
    }
    
    public QueryStringSubscription getBySubscriptionId(String id){
        return subscriptions.get(id);
    }
    
    public void addAll(Collection<QueryStringSubscription> subs){
        subs.stream().forEach((s) -> this.subscriptions.put(s.getId(), s));
    }
    
    @Retryable(maxAttempts = 28800, backoff = @Backoff(delay = 3000), include = ConnectException.class)
    public void initializeSubscriptions() throws IOException{
        Logger logger = LogManager.getLogger(this.getClass());
        logger.info("Initializing Subscriptions");
        this.subscriptions.clear();
        List<QueryStringSubscription> subs = client.getAll();
        this.addAll(subs);
        logger.debug("Loaded subscriptions: " + subs.stream().map(s->s.getId()).collect(Collectors.joining(", ")));
    }
    
    public void receiveSubscriptionUpdate(SubscriptionEvent update) {
        Logger logger = LogManager.getLogger(this.getClass());
        switch (update.getSubscriptionAction()) {
            case SubscriptionAction.SUBSCRIBE:
                QueryStringSubscription subscription;
                try {
                    subscription = client.getById(update.getSubscriptionId());
                } catch (IOException ex) {
                    throw new RuntimeException("Cannot retrieve subscription " + update.getSubscriptionId() + " for the update.", ex);
                }
                logger.debug("Adding/Updating subscription [" + subscription.getId() + "] to the subscription registry");
                this.add(subscription);
                break;
            case SubscriptionAction.UNSUBSCRIBE:
                logger.debug("Removing subscription " + update.getSubscriptionId() + " from the registry.");
                this.remove(update.getSubscriptionId());
                break;
            default:
                break;
        }
    }
    
}
