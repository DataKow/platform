package org.datakow.apps.subscriptionwebservice;

import org.datakow.catalogs.metadata.webservice.configuration.EnableMetadataCatalogWebServiceClient;
import org.datakow.configuration.application.DatakowApplication;
import org.datakow.messaging.events.configuration.EnableEventsSender;
import org.datakow.messaging.notification.configuration.EnableNotificationReceiver;
import org.datakow.security.EnableHardCodedAuthentication;
import springfox.documentation.swagger2.annotations.EnableSwagger2;

/**
 *
 * @author kevin.off
 */
@EnableEventsSender
@EnableNotificationReceiver
@EnableHardCodedAuthentication
@EnableSwagger2
@EnableMetadataCatalogWebServiceClient
public class Application extends DatakowApplication {
    
    public static void main(String[] args) {
        Application.run(Application.class, args);
    }

}
