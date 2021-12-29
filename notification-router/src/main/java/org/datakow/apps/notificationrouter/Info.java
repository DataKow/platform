package org.datakow.apps.notificationrouter;

import org.datakow.catalogs.subscription.QueryStringSubscription;
import java.io.IOException;
import java.util.Collection;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author kevin.off
 */
@RestController
public class Info {
    
    @Autowired
    private SubscriptionRegistry subscriptionRegistry;
    
    @RequestMapping(value = "/subscriptions", method = RequestMethod.GET)
    public Collection<QueryStringSubscription> getSubscriptions(){
        return subscriptionRegistry.getAll();
    }
    
    @RequestMapping(value = "/subscriptions/refresh", method = RequestMethod.POST)
    public Collection<QueryStringSubscription> refreshSubscriptions() throws IOException{
        subscriptionRegistry.initializeSubscriptions();
        return subscriptionRegistry.getAll();
    }
    
}
