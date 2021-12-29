package org.datakow.apps.notificationrouter;


import org.datakow.catalogs.metadata.webservice.configuration.EnableMetadataCatalogWebServiceClient;
import org.datakow.catalogs.subscription.webservice.configuration.EnableSubscriptionWebServiceClient;
import org.datakow.configuration.application.DatakowApplication;
import org.datakow.messaging.events.configuration.EnableEventsReceiver;
import org.datakow.messaging.notification.configuration.EnableNotificationSender;
import org.springframework.retry.annotation.EnableRetry;

/**
 *
 * @author kevin.off
 */
@EnableMetadataCatalogWebServiceClient
@EnableSubscriptionWebServiceClient
@EnableNotificationSender
@EnableEventsReceiver
@EnableRetry
public class Application extends DatakowApplication{
    public static void main(String[] args){
        Application.run(Application.class, args);
    }
}
