package org.datakow.apps.metadatacatalogwebservice;

import org.datakow.catalogs.metadata.database.MongoDBMetadataCatalogManagementDao;
import org.datakow.catalogs.metadata.database.configuration.EnableMetadataCatalogMongoClient;
import org.datakow.catalogs.metadata.webservice.configuration.EnableMetadataCatalogWebServiceClient;
import org.datakow.configuration.application.DatakowApplication;
import org.datakow.messaging.events.configuration.EnableEventsReceiver;
import org.datakow.messaging.events.configuration.EnableEventsSender;
import org.datakow.security.EnableHardCodedAuthentication;
import org.springframework.boot.CommandLineRunner;

/**
 *
 * @author kevin.off
 */
@EnableEventsSender
@EnableHardCodedAuthentication
@EnableMetadataCatalogMongoClient
@EnableMetadataCatalogWebServiceClient
@EnableEventsReceiver
public class Application extends DatakowApplication implements CommandLineRunner {

    MongoDBMetadataCatalogManagementDao catalogClient;

    public Application(MongoDBMetadataCatalogManagementDao client){
        catalogClient = client;
    }

    public static void main(String[] args) {
        Application.run(Application.class, args);
    }

    @Override
    public void run(String... args) throws Exception {
        catalogClient.initializeDatakow();
    }
}
