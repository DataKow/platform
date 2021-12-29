package org.datakow.apps.objectcatalogwebservice;

import java.util.logging.Logger;

import org.datakow.catalogs.object.database.configuration.EnableObjectCatalogMongoClient;
import org.datakow.catalogs.object.webservice.configuration.EnableObjectCatalogWebServiceClient;
import org.datakow.configuration.application.DatakowApplication;
import org.datakow.messaging.events.configuration.EnableEventsSender;
import org.datakow.security.EnableHardCodedAuthentication;


 


/**
 *
 * @author kevin.off
 */
@EnableEventsSender
@EnableHardCodedAuthentication
@EnableObjectCatalogMongoClient
@EnableObjectCatalogWebServiceClient
public class Application extends DatakowApplication {
     
    public static void main(String[] args) {
        Application.run(Application.class, args);
    }
}
