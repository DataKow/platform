package org.datakow.apps.metadatacatalogwebservice;

import com.fasterxml.jackson.core.JsonProcessingException;

import org.datakow.catalogs.metadata.database.MongoDBMetadataCatalogDao;
import org.datakow.catalogs.metadata.database.MongoDBMetadataCatalogManagementDao;
import org.datakow.catalogs.metadata.database.MongoDBTestHarness;
import org.datakow.catalogs.metadata.webservice.configuration.MetadataCatalogWebServiceClientConfigurationProperties;
import org.datakow.configuration.rabbit.RabbitClient;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.DotNotationMap;
import org.datakow.messaging.events.CatalogEventsReceiverClient;
import org.datakow.messaging.events.CatalogEventsSenderClient;

import org.datakow.messaging.events.EventsRoutingKeyBuilder;
import org.datakow.messaging.events.configuration.EventsSenderConfiguration.CatalogEventsSenderGateway;

import java.io.IOException;
import java.util.List;

import org.datakow.catalogs.metadata.BulkResult;
import org.datakow.catalogs.metadata.Catalog;
import org.datakow.catalogs.metadata.CatalogRegistry;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.mockito.Mockito;
import static org.mockito.Mockito.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;
import org.springframework.context.annotation.Bean;
import org.springframework.data.mongodb.MongoDatabaseFactory;

/**
 *
 * @author kevin.off
 */
@org.springframework.boot.test.context.TestConfiguration
@SpringBootApplication(exclude = {ErrorMvcAutoConfiguration.class, MongoAutoConfiguration.class, MongoDataAutoConfiguration.class, RabbitAutoConfiguration.class})
public class TestConfiguration {
    
    private final String recordIdentifier = "39a86168-2ffd-4d9c-b836-e973c1e618e5";
    
    @Bean
    public MongoDatabaseFactory databaseFactory() {
        return mock(MongoDatabaseFactory.class);
    }

    @Bean
    public RabbitClient rabbitClient(){
        return mock(RabbitClient.class);   
    }
    
    @Bean
    public CatalogEventsSenderGateway gateway(){
       return mock(CatalogEventsSenderGateway.class);
    }    
    
    @Bean
    public CatalogEventsReceiverClient receiverClient(){
        CatalogEventsReceiverClient client = mock(CatalogEventsReceiverClient.class);
        when(client.startReceivingEventsIndividual(anyString())).thenReturn(null);
        return client;
    }
    
    @Bean
    public EventsRoutingKeyBuilder builder(){
        return new EventsRoutingKeyBuilder("v1", "dev", "test-app");
    }
    
    @Bean
    public MongoDBMetadataCatalogDao metadataCatalogDao(MongoDBTestHarness testHarness) throws IOException{
        return testHarness.getMockDao();
    }
    
    @Bean
    public MongoDBTestHarness testHarness(){
        return new MongoDBTestHarness();
    }
    
    @Bean
    public MongoDBMetadataCatalogManagementDao managementDao(){
        return mock(MongoDBMetadataCatalogManagementDao.class);
    }
    
    @Bean
    public CatalogEventsSenderClient eventsSendingClient(){
        CatalogEventsSenderClient client = Mockito.mock(CatalogEventsSenderClient.class);
        return client;
    }
    
    @Bean
    public CatalogRegistry catalogRegistry() throws JsonProcessingException{
        CatalogRegistry registry = mock(CatalogRegistry.class);
        when(registry.getByCatalogIdentifier(anyString())).thenAnswer(i -> {
                Catalog catalog = new Catalog("metadata");
                catalog.setCatalogIdentifier(i.getArgument(0, String.class));
                catalog.setCollectionName(i.getArgument(0, String.class));
                return catalog;
            }
        );
        when(registry.catalogExists(anyString())).thenReturn(true);
        return registry;
    }
    
    @Bean
    public MetadataCatalogWebServiceClientConfigurationProperties props(){
        MetadataCatalogWebServiceClientConfigurationProperties props = new MetadataCatalogWebServiceClientConfigurationProperties();
        
        return props;
    }
    
    public MetadataCatalogRecord mockRecordWithDistanceField(){
        MetadataCatalogRecord record = mockRecord();
        DotNotationMap metaDoc = new DotNotationMap();
        metaDoc.setProperty("distanceFromQueryPoint", 500.255);
        return record;
    }
    
    public MetadataCatalogRecord mockRecord(){
        return testHarness().getMockRecord(0);
    }
    
    public MetadataCatalogRecord mockRecord(int index){
        return testHarness().getMockRecord(index);
    }
    
    public List<MetadataCatalogRecord> mockRecords(int count){
        return testHarness().getMockRecords(count);
    }
    
    public CatalogIdentity mockObjectCatalogIdentity(){
        return new CatalogIdentity("DATAKOW_OBJECTS", "a5a8616a-bffc-4ad54-c8a6-c873c4e618b3");
    }
    
    public DotNotationMap mockDocument(){
        return mockRecord().getDocument();
    }
    
    
    public List<BulkResult> getBulkResultUpsert(){
        return testHarness().getBulkResultUpsert();
    }
    
    public List<BulkResult> getBulkResultCreate(){
        return testHarness().getBulkResultCreate();
    }
    
    public List<BulkResult> getBulkResultUpdateOne(){
        return testHarness().getBulkResultUpdateOne();
    }
    
}
