package org.datakow.apps.objectcatalogwebservice;



import com.fasterxml.jackson.core.JsonParseException;
import com.mongodb.MongoTimeoutException;
import com.mongodb.client.MongoCursor;

import org.datakow.apps.objectcatalogwebservice.exception.UnauthorizedException;
import org.datakow.catalogs.object.database.MongoDBObjectCatalogDao;
import org.datakow.catalogs.object.database.MongoRecordPropertyStream;
import org.datakow.catalogs.object.database.ObjectDataCoherence;
import org.datakow.catalogs.object.webservice.configuration.ObjectCatalogWebServiceClientConfigurationProperties;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.messaging.events.CatalogEventsSenderClient;
import org.datakow.messaging.events.EventsRoutingKeyBuilder;
import org.datakow.messaging.events.configuration.EventsSenderConfiguration.CatalogEventsSenderGateway;
import org.datakow.messaging.events.events.Event;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;
import java.util.Calendar;

import org.bson.Document;
import org.datakow.catalogs.object.ObjectCatalogRecord;
import org.datakow.catalogs.object.ObjectCatalogRecordInput;
import org.mockito.Mockito;
import org.springframework.context.annotation.Bean;

import static org.mockito.Mockito.*;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.amqp.RabbitAutoConfiguration;
import org.springframework.boot.autoconfigure.data.mongo.MongoDataAutoConfiguration;
import org.springframework.boot.autoconfigure.mongo.MongoAutoConfiguration;
import org.springframework.boot.autoconfigure.web.servlet.error.ErrorMvcAutoConfiguration;

/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

/**
 *
 * @author kevin.off
 */
@org.springframework.boot.test.context.TestConfiguration
@SpringBootApplication(exclude = {ErrorMvcAutoConfiguration.class, MongoAutoConfiguration.class, MongoDataAutoConfiguration.class, RabbitAutoConfiguration.class})
public class TestConfiguration {
    
    private final String uuidRegex = "[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}";
    
    @Bean
    public CatalogEventsSenderGateway gateway(){
        return (Event event, String routingKey, String requestId, String correlationId) -> {
        };
    }    
    
    @Bean
    public EventsRoutingKeyBuilder builder(){
        return new EventsRoutingKeyBuilder("v1", "dev", "test-app");
    }
    
    @Bean
    //39a86168-2ffd-4d9c-b836-e973c1e618e5
    public MongoDBObjectCatalogDao objectCatalogDao() throws IOException{
        ///catalogs/DATAKOW_OBJECTS/objects/jim
        MongoDBObjectCatalogDao dao = Mockito.mock(MongoDBObjectCatalogDao.class);
        when(dao.create(any(ObjectCatalogRecordInput.class))).thenReturn(mockCatalogIdentity());
        when(dao.getById(eq("MONGO_EXCEPTION"), any(ObjectDataCoherence.class))).thenThrow(new MongoTimeoutException("Test Mongo Exception"));
        when(dao.getById(eq("NOT_FOUND"), any(ObjectDataCoherence.class))).thenReturn(null);
        when(dao.getById(eq("RUNTIME_EXCEPTION"), any(ObjectDataCoherence.class))).thenThrow(new RuntimeException("Test Runtime Exception"));
        when(dao.getById(eq("JSON_PROCESSING_EXCAPTION"), any(ObjectDataCoherence.class))).thenThrow(new JsonParseException(null, "Test Json Processing Exception"));
        when(dao.getById(eq("UNAUTHORIZED_EXCEPTION"), any(ObjectDataCoherence.class))).thenThrow(new UnauthorizedException("secret"));
        when(dao.getById(matches(uuidRegex), any(ObjectDataCoherence.class))).thenAnswer(i -> mockRecord());
        when(dao.getByQuery(anyString(), any(ObjectDataCoherence.class))).thenAnswer(i -> new MyRecordPropertyStream("",null,0,null));
        when(dao.getByQuery(anyString(), anyString(), any(ObjectDataCoherence.class))).thenAnswer(i -> new MyRecordPropertyStream("",null,0,null));
        when(dao.getByQuery(anyString(), anyInt(), any(ObjectDataCoherence.class))).thenAnswer(i -> new MyRecordPropertyStream("",null,0,null));
        when(dao.getByQuery(anyString(), anyString(), anyInt(), any(ObjectDataCoherence.class))).thenAnswer(i -> new MyRecordPropertyStream("",null,0,null));
        when(dao.getObjectCatalogNames()).thenReturn(Arrays.asList("DATAKOW_OBJECTS"));
        return dao;
    }

    @Bean
    public CatalogEventsSenderClient eventsSendingClient(){
        CatalogEventsSenderClient client = Mockito.mock(CatalogEventsSenderClient.class);
        return client;
    }
    
    @Bean
    public ObjectCatalogWebServiceClientConfigurationProperties catalogConfig(){
        return new ObjectCatalogWebServiceClientConfigurationProperties();
    }
    
    private ObjectCatalogRecord mockRecord() throws UnsupportedEncodingException{
        ObjectCatalogRecord record = new ObjectCatalogRecord();
        record.setContentType("text/plain");
        record.setId("39a86168-2ffd-4d9c-b836-e973c1e618e5");
        Calendar calendar = Calendar.getInstance();
        calendar.set(2017, 3, 24, 12, 0, 0);
        record.setPublishDate(calendar.getTime());
        record.setPublisher("bob");
        record.setRealm("secret");
        record.setTags(Arrays.asList("Tagone", "tag2"));
        record.setContentLength(9);
        record.setMetadataCatalogIdentifiers("catalogone", "catalog2");
        InputStream stream = new ByteArrayInputStream("Test Data".getBytes("UTF-8"));
        record.setData(stream);
        return record;
    }
    
    private CatalogIdentity mockCatalogIdentity(){
        return new CatalogIdentity("DATAKOW_OBJECTS", "39a86168-2ffd-4d9c-b836-e973c1e618e5");
    }
    
    public class MyRecordPropertyStream extends MongoRecordPropertyStream<String>{
        
        int count = 0;
        
        public MyRecordPropertyStream(String propertyName, String fiql, int limit, MongoCursor<Document> cursor) {
            super(propertyName, fiql, limit, cursor);
        }
        
        @Override
        public boolean hasNext(){
            switch (count) {
                case 0:
                    return true;
                case 1:
                    return false;
                default:
                    throw new IllegalStateException("The count is off for some reason");
            }
        }
        
        @Override
        public String next(){
            count++;
            return mockCatalogIdentity().getRecordIdentifier();
        }
        
        @Override
        public void close(){
            
        }
        
    }
    
}
