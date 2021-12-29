package org.datakow.apps.objectcatalogwebservice;

import com.fasterxml.jackson.core.JsonParseException;
import com.mongodb.MongoException;
import org.datakow.apps.objectcatalogwebservice.exception.InvalidRequestParameterException;
import org.datakow.apps.objectcatalogwebservice.exception.SourceRecordNotFoundException;
import org.datakow.apps.objectcatalogwebservice.exception.UnauthorizedException;
import org.datakow.security.EnableHardCodedAuthentication;
import org.junit.Before;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;

/**
 *
 * @author kevin.off
 */
@SpringBootTest(classes = TestConfiguration.class)
@WebAppConfiguration
@RunWith(SpringRunner.class)
@TestPropertySource(properties = {"spring.cloud.bootstrap.enabled=false"})
@EnableHardCodedAuthentication
@WithMockUser(username = "datakow", roles = "ADMIN")
public class RequestControllerTest {
    
    private final String recordIdentifier = "39a86168-2ffd-4d9c-b836-e973c1e618e5";
    
    @Autowired
    TestConfiguration config;
    
    @Autowired
    private WebApplicationContext webApplicationContext;
    
    private MockMvc mvc;
    
    public RequestControllerTest() {
    }

    @Before
    public void setup() throws Exception {
        MockitoAnnotations.initMocks(config);
        
        this.mvc = MockMvcBuilders.webAppContextSetup(webApplicationContext).build();
    }
    
    @Test
    public void testDefaultPage() throws Exception {
        
        this.mvc.perform(get("/"))
                .andExpect(status().is(200))
                .andExpect(content().string(""));        
    }

    @Test
    public void testCreate() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.TEXT_PLAIN);
        headers.set("realm", "public");
        
        this.mvc.perform(post("/catalogs/DATAKOW_OBJECTS/objects").headers(headers))
                .andExpect(status().is(201))
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                .andExpect(header().string("Location", "/catalogs/DATAKOW_OBJECTS/objects/" + recordIdentifier))
                .andExpect(content().string("{\"id\":\"" + recordIdentifier + "\"}"));
    }
    
    @Test
    public void testCreateNoContentType() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        headers.set("realm", "public");
        
        this.mvc.perform(post("/catalogs/DATAKOW_OBJECTS/objects").headers(headers))
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().is(415));
    }
    
    
    

    @Test
    public void testDelete() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        
        this.mvc.perform(delete("/catalogs/DATAKOW_OBJECTS/objects/" + recordIdentifier).headers(headers))
                .andExpect(status().is(200))
                .andExpect(content().string(""));
    }

    @Test
    public void testFindById() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        
        this.mvc.perform(get("/catalogs/DATAKOW_OBJECTS/objects/" + recordIdentifier).headers(headers))
                .andExpect(header().string("Content-Type", MediaType.TEXT_PLAIN_VALUE))
                .andExpect(status().is(200))
                .andExpect(header().string("Content-Length", "9"))
                .andExpect(content().string("Test Data"));
    }
    
    @Test
    public void testFindByIdNotFound() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        
        this.mvc.perform(get("/catalogs/DATAKOW_OBJECTS/objects/NOT_FOUND").headers(headers))
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().is(404));
    }

    @Test
    public void testFindByIdMongoException() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        
        this.mvc.perform(get("/catalogs/DATAKOW_OBJECTS/objects/MONGO_EXCEPTION").headers(headers))
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().is(500))
                .andExpect(content().json(getMongoExceptionMessage()));
    }
    
    @Test
    public void testFindByIdJsonParseException() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        
        this.mvc.perform(get("/catalogs/DATAKOW_OBJECTS/objects/JSON_PROCESSING_EXCAPTION").headers(headers))
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().is(500))
                .andExpect(content().json(getJsonProcessingExceptionMessage()));
    }
    
    @Test
    public void testFindByIdRuntimeException() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        
        this.mvc.perform(get("/catalogs/DATAKOW_OBJECTS/objects/RUNTIME_EXCEPTION").headers(headers))
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().is(500))
                .andExpect(content().json(getRuntimeExceptionMessage()));
    }
    
    @Test
    @WithMockUser(username = "jim", roles = "USER")
    public void testFindByIdUnauthorized() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        
        this.mvc.perform(get("/catalogs/DATAKOW_OBJECTS/objects/UNAUTHORIZED_EXCEPTION").headers(headers))
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE))
                .andExpect(status().is(403))
                .andExpect(content().json(getUnauthorizedExceptionMessage()));
    }
    
    @Test
    public void testFindByQueryLegacy() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        
        this.mvc.perform(get("/catalogs/DATAKOW_OBJECTS/objects?s=Realm!=public&collectionFormat=legacy").headers(headers))
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().is(200))
                .andExpect(content().json(getLegacyResult()));
    }
    
    @Test
    public void testFindByQueryJson() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        
        this.mvc.perform(get("/catalogs/DATAKOW_OBJECTS/objects?s=Realm!=public&collectionFormat=json").headers(headers))
                .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                .andExpect(status().is(200))
                .andExpect(content().json(getJsonResult()));
    }
    
    @Test
    public void testFindByQueryMultipartMixed() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        headers.set("boundary", "41914bff-8c4a-41a4-a2b1-4ba6172c83f9");
        this.mvc.perform(get("/catalogs/DATAKOW_OBJECTS/objects?s=Realm!=public&collectionFormat=multipart_mixed").headers(headers))
                .andExpect(header().string("Content-Type", "multipart/mixed;boundary=41914bff-8c4a-41a4-a2b1-4ba6172c83f9"))
                .andExpect(status().is(200))
                .andExpect(content().string(getMultipartMixedResult()));
    }
    
    @Test
    public void testFindOneByQuery() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        this.mvc.perform(get("/catalogs/DATAKOW_OBJECTS/objects?s=Realm!=public&findOne=true").headers(headers))
                .andExpect(header().string("Content-Type", MediaType.TEXT_PLAIN_VALUE))
                .andExpect(status().is(200))
                .andExpect(header().string("Content-Length", "9"))
                .andExpect(content().string("Test Data"));
    }
    
    private String getLegacyResult(){
        return "[\"" + recordIdentifier + "\"]";
    }
    
    private String getJsonResult(){
        return "[{\r\n" +
        "\"Record-Identifier\":\"39a86168-2ffd-4d9c-b836-e973c1e618e5\",\r\n" +
        "\"Content-Type\":\"text/plain\",\r\n" +
        "\"Publisher\":\"bob\",\r\n" +
        "\"Realm\":\"secret\",\r\n" +
        "\"Tags\":[\"Tagone\",\"tag2\"],\r\n" +
        "\"Metadata-Catalog-Identifiers\":[\"catalogone\",\"catalog2\"],\r\n" +        
        "\"Publish-Date\":\"Mon, 24 Apr 2017 17:00:00 GMT\",\r\n" +
        "\"Data\":\"VGVzdCBEYXRh\"}]";
    }
    
    private String getMultipartMixedResult(){
        return "\r\n" +
        "--41914bff-8c4a-41a4-a2b1-4ba6172c83f9\r\n" +
        "Record-Identifier: 39a86168-2ffd-4d9c-b836-e973c1e618e5\r\n" +
        "Content-Type: text/plain\r\n" +
        "Publisher: bob\r\n" +
        "Realm: secret\r\n" +
        "Tags: Tagone,tag2\r\n" +
        "Metadata-Catalog-Identifiers: catalogone,catalog2\r\n"+
        "Publish-Date: Mon, 24 Apr 2017 17:00:00 GMT\r\n" +
        "Content-Length: 9\r\n" + 
        "\r\n" +
        "Test Data\r\n" +
        "--41914bff-8c4a-41a4-a2b1-4ba6172c83f9--\r\n";
    }
    
    private String getBadRequestParameterMessage(String parameter){
        InvalidRequestParameterException ex = new InvalidRequestParameterException(parameter);
        return CustomExceptionHandler.buildErrorMessage(ex.getCode(), ex.getMessage());
    }
    
    private String getMongoExceptionMessage(){
        return (String)new CustomExceptionHandler().mongoExceptionHandler(new MongoException("")).getBody();
    }
    
    private String getJsonProcessingExceptionMessage(){
        return (String)new CustomExceptionHandler().jsonProcessingExceptionHandler(new JsonParseException(null, "")).getBody();
    }
    
    private String getRuntimeExceptionMessage(){
        return (String)new CustomExceptionHandler().defaultExceptionHandler(new RuntimeException("")).getBody();
    }
    
    private String getSourceNotFoundExceptionMessage(String recordId){
        SourceRecordNotFoundException ex = new SourceRecordNotFoundException(recordId);
        return CustomExceptionHandler.buildErrorMessage(ex.getCode(), ex.getMessage());
    }
    
    private String getUnauthorizedExceptionMessage(){
        UnauthorizedException ex = new UnauthorizedException("secret");
        return CustomExceptionHandler.buildErrorMessage(ex.getCode(), ex.getMessage());
    }
}
