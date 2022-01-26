package org.datakow.apps.mockdatapump;

import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Random;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.web.client.ResourceAccessException;
import org.springframework.web.client.RestClientResponseException;

import org.datakow.catalogs.metadata.webservice.MetadataCatalogWebserviceClient;
import org.datakow.catalogs.metadata.webservice.configuration.EnableMetadataCatalogWebServiceClient;
import org.datakow.catalogs.object.ObjectCatalogRecordInput;
import org.datakow.catalogs.object.webservice.ObjectCatalogWebserviceClient;
import org.datakow.catalogs.object.webservice.configuration.EnableObjectCatalogWebServiceClient;
import org.datakow.configuration.application.DatakowApplication;
import org.datakow.core.components.DotNotationMap;

/**
 *
 * @author kevin.off
 */
@EnableMetadataCatalogWebServiceClient
@EnableObjectCatalogWebServiceClient
@EnableScheduling
public class Application extends DatakowApplication {
    
    private MetadataCatalogWebserviceClient metadataClient;
    private ObjectCatalogWebserviceClient objectClient;
    
    @Autowired
    public Application(MetadataCatalogWebserviceClient metaClient, ObjectCatalogWebserviceClient objClient)
    {
        metadataClient = metaClient;
        objectClient = objClient;
    }

    public static void main(String[] args) {
        Application.run(Application.class, args);
    }

    @Scheduled(fixedDelay = 5000L, initialDelay = 0L)
    public void pumpData() throws ResourceAccessException, RestClientResponseException, JsonProcessingException {

        var someMetadata = new DotNotationMap();
        someMetadata.setProperty("firstName", "Kevin");
        someMetadata.setProperty("lastName", "Off");
        someMetadata.setProperty("ssn", "665-45-4421");
        someMetadata.setProperty("birthDate", "1986-06-02T00:00:00Z");
        someMetadata.setProperty("active", false);
        someMetadata.setProperty("Random", new Random().nextInt(152221));
        
        var identity = metadataClient.create("stuff", someMetadata);

        var record = new ObjectCatalogRecordInput();
        record.setContentType("application/json");
        record.addObjectMetadataIdentity(identity);
        record.setPublisher("datakow");
        record.setData("Some data that is super important: " + new Random().nextInt(10000));

        objectClient.create("DATAKOW_OBJECTS", record);

    }
}
