package org.datakow.apps.metadatacatalogwebservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import org.datakow.apps.metadatacatalogwebservice.RequestController.ParsedInputStream;
import org.datakow.apps.metadatacatalogwebservice.exception.InvalidRequestBodyException;
import org.datakow.catalogs.metadata.jsonpatch.JsonPatchOperation;
import org.datakow.catalogs.metadata.webservice.MetadataCatalogWebserviceRequest;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.DotNotationList;
import org.datakow.core.components.DotNotationMap;
import org.datakow.security.EnableHardCodedAuthentication;
import java.io.ByteArrayInputStream;
import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.apache.commons.io.IOUtils;
import org.datakow.catalogs.metadata.BulkResult;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;

import static org.junit.Assert.*;
import org.junit.Test;
import org.junit.Before;
import org.junit.Rule;
import org.junit.runner.RunWith;
import org.mockito.MockitoAnnotations;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.restdocs.JUnitRestDocumentation;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;
import org.springframework.test.context.web.WebAppConfiguration;
import org.springframework.test.web.servlet.MockMvc;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.restdocs.mockmvc.RestDocumentationRequestBuilders.put;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.content;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.header;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;
import org.springframework.web.context.WebApplicationContext;
//import static org.springframework.restdocs.mockmvc.MockMvcRestDocumentation.documentationConfiguration;

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
    
    
    @Autowired
    TestConfiguration config;
    
    @Autowired
    private WebApplicationContext webApplicationContext;
    
    @Autowired
    RequestController controller;
    
    private MockMvc mvc;
    
    @Autowired
    TestConfiguration testConfig;
    
    @Rule
    public JUnitRestDocumentation restDocumentation = new JUnitRestDocumentation("target/generated-snippets");
    
    
    public RequestControllerTest() {
    }
    
    @Before
    public void setup(){
        MockitoAnnotations.initMocks(config);
        this.mvc = MockMvcBuilders
                .webAppContextSetup(webApplicationContext)
                //.apply(documentationConfiguration(restDocumentation).snippets().withEncoding("UTF-8"))
                .build();
        
    }
    
    @Test
    public void testDefaultPage() throws Exception {
        
        this.mvc.perform(get("/"))
                .andExpect(status().is(200))
                .andExpect(content().string(""));        
        
    }
    
    @Test
    public void testUpdateByIdUsingRecord() throws Exception {
        
        //Technically this is ILLEGAL...but secretly supported.
        HttpHeaders headers = new HttpHeaders();
        MetadataCatalogRecord record = testConfig.mockRecord();
        //User cannot send a record with a different ID as the URL
        //The ID in the url will prevail!
        record.getStorage().setId(UUID.randomUUID().toString());
        this.mvc.perform(put("/catalogs/upsertByQueryUpdateRecord/records/" + testConfig.mockRecord().getStorage().getId())
                            .headers(headers)
                            .content(record.toJson().getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Location", "/catalogs/upsertByQueryUpdateRecord/records/" + testConfig.mockRecord().getStorage().getId()))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(content().json("{\"id\":\"" + testConfig.mockRecord().getStorage().getId() + "\"}"));
        
    }
    
    @Test
    public void testUpdateByIdUsingDocument() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        this.mvc.perform(put("/catalogs/DATAKOW_CATALOG/records/" + testConfig.mockRecord().getStorage().getId())
                            .headers(headers)
                            .content(testConfig.mockDocument().toJson().getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Location", "/catalogs/DATAKOW_CATALOG/records/" + testConfig.mockRecord().getStorage().getId()))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(content().json("{\"id\":\"" + testConfig.mockRecord().getStorage().getId() + "\"}"));
    }
    
    @Test
    public void testUpdateOneByQueryUsingDocument() throws Exception {
        HttpHeaders headers = new HttpHeaders();
        this.mvc.perform(put("/catalogs/DATAKOW_CATALOG/records?s=Doc.stuff==things")
                            .headers(headers)
                            .content(testConfig.mockDocument().toJson().getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE));
    }
    
    @Test
    public void testUpdateOneByQueryUsingRecord() throws Exception {
        //Technically this is ILLEGAL...but secretly supported.
        HttpHeaders headers = new HttpHeaders();
        this.mvc.perform(put("/catalogs/DATAKOW_CATALOG/records?s=Doc.stuff==things")
                            .headers(headers)
                            .content(testConfig.mockRecord().toJson().getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE));
    }
    
    @Test
    public void testUpdateByParameterizedFilterUsingRecords() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        headers.add("Operation-Type", "bulk");
        this.mvc.perform(put(new URI("/catalogs/upsertManyUpdateRecords/records?filter=Storage.Record-Identifier%3D%3D%7BStorage.Record-Identifier%7D"))
                            .headers(headers)
                            .content(getRecordsJsonArray(5).getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE));
                        //.andExpect(content().json(getBulkResultUpsertString()));
    }
    
    @Test
    public void testUpdateByParameterizedFilterUsingDocuments() throws Exception {
        
        //Just to make sure that nothing happens if you try this.
        HttpHeaders headers = new HttpHeaders();
        headers.add("Operation-Type", "bulk");
        this.mvc.perform(put(new URI("/catalogs/upsertManyUpdateRecords/records?filter=Storage.Record-Identifier%3D%3D%7BStorage.Record-Identifier%7D"))
                            .headers(headers)
                            .content(getDocumentsJsonArray(5).getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(content().json("[]"));
    }
    
    @Test
    public void testCreateUsingDocument() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        //Put this identifier in the header to make sure it uses it
        headers.add("Record-Identifier", testConfig.mockRecord().getStorage().getId());
        this.mvc.perform(post("/catalogs/DATAKOW_CATALOG/records")
                            .headers(headers)
                            .content(testConfig.mockDocument().toJson().getBytes()))
                        .andExpect(status().is(201))
                        .andExpect(header().string("Location", "/catalogs/DATAKOW_CATALOG/records/" + testConfig.mockRecord().getStorage().getId()))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(content().json("{\"id\":\"" + testConfig.mockRecord().getStorage().getId() + "\"}"));
    }
    
    @Test
    public void testCreateUsingRecord() throws Exception {
        //Technically this is ILLEGAL...but secretly supported
        HttpHeaders headers = new HttpHeaders();
        //Put this random identifier in the header to make sure it does not use it.
        headers.add("Record-Identifier", UUID.randomUUID().toString());
        this.mvc.perform(post("/catalogs/DATAKOW_CATALOG/records")
                            .headers(headers)
                            .content(testConfig.mockRecord().toJson().getBytes()))
                        .andExpect(status().is(201))
                        .andExpect(header().string("Location", "/catalogs/DATAKOW_CATALOG/records/" + testConfig.mockRecord().getStorage().getId()))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(content().json("{\"id\":\"" + testConfig.mockRecord().getStorage().getId() + "\"}"));
    }
    
    @Test
    public void testCreateBadBody() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        //Put this random identifier in the header to make sure it does not use it.
        headers.add("Record-Identifier", UUID.randomUUID().toString());
        this.mvc.perform(post("/catalogs/DATAKOW_CATALOG/records")
                            .headers(headers)
                            .content("asdf".getBytes()))
                        .andExpect(status().is(400))
                        .andExpect(content().json(CustomExceptionHandler.buildErrorMessage("BAD_REQUEST_BODY", new InvalidRequestBodyException("Your request body could not be parsed.").getLocalizedMessage())));
    }
    
    @Test
    public void testCreateBulkUsingRecords() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        //Put this random identifier in the header to make sure it does not use it.
        headers.add("Record-Identifier", UUID.randomUUID().toString());
        headers.add("Operation-Type", "bulk");
        this.mvc.perform(post("/catalogs/DATAKOW_CATALOG/records")
                            .headers(headers)
                            .content(getRecordsJsonArray(5).getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(content().json(getBulkResultCreateString()));
    }
    
    @Test
    public void testCreateBulkUsingDocuments() throws Exception {
        
        //Just making sure nothing happens if you do this.
        HttpHeaders headers = new HttpHeaders();
        //Put this random identifier in the header to make sure it does not use it.
        headers.add("Record-Identifier", UUID.randomUUID().toString());
        headers.add("Operation-Type", "bulk");
        this.mvc.perform(post("/catalogs/DATAKOW_CATALOG/records")
                            .headers(headers)
                            .content(getDocumentsJsonArray(5).getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(content().json("[]"));
    }
    
    @Test
    public void testCreateBulkBadBody() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        //Put this random identifier in the header to make sure it does not use it.
        headers.add("Record-Identifier", UUID.randomUUID().toString());
        headers.add("Operation-Type", "bulk");
        this.mvc.perform(post("/catalogs/DATAKOW_CATALOG/records")
                            .headers(headers)
                            .content("{\"stuff\":123}".getBytes()))
                        .andExpect(status().is(400))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_UTF8_VALUE))
                        .andExpect(content().json(CustomExceptionHandler.buildErrorMessage("BAD_REQUEST_BODY", new InvalidRequestBodyException("A POST request with the Operation-Type=bulk requires an array of JSON objects in the request body.").getLocalizedMessage())));
    }

    @Test
    public void testDelete() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        this.mvc.perform(delete("/catalogs/DATAKOW_CATALOG/records/" + testConfig.mockRecord().getStorage().getId())
                            .headers(headers))
                        .andExpect(status().is(200));
        
    }
    
    @Test
    public void testDeleteByQuery() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        this.mvc.perform(delete("/catalogs/DATAKOW_CATALOG/records/?s=property==value")
                            .headers(headers))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Num-Deleted", "1"));
        
    }

    @Test
    public void testFindByQueryRegular() throws Exception {
        
        String url = MetadataCatalogWebserviceRequest.builder()
                .withQuery("property==value")
                .withSort(Arrays.asList("Property DESC"))
                .withLimit(3)
                .toUrl("/catalogs/DATAKOW_CATALOG/records");
        
        HttpHeaders headers = new HttpHeaders();
        List<MetadataCatalogRecord> records = testConfig.mockRecords(5);
        String mockRecordsJson = new DotNotationList(records).toJson();
        this.mvc.perform(get(url)
                        .headers(headers))
                        .andExpect(status().is(200))
                        .andExpect(content().json(mockRecordsJson));
        
    }
    
    @Test
    public void testFindByQueryAggregateGeoNear() throws Exception {
        
        String url = MetadataCatalogWebserviceRequest.builder()
                .withQuery("property==value")
                .withSort(Arrays.asList("Property DESC"))
                .withLimit(10)
                .withNear("(-96,45,100)")
                .toUrl("/catalogs/DATAKOW_CATALOG/records");
        
        HttpHeaders headers = new HttpHeaders();
        this.mvc.perform(get(url)
                            .headers(headers))
                        .andExpect(status().is(200))
                        .andExpect(content().json(getRecordsJsonArray(5)));
        
    }
    
    @Test
    public void testFindByQueryGroup() throws Exception {
        
        String url = MetadataCatalogWebserviceRequest.builder()
                .withQuery("property==value")
                .withSort(Arrays.asList("Property DESC"))
                .withLimit(10)
                .withGroupFunctions("first(Property)")
                .toUrl("/catalogs/DATAKOW_CATALOG/records");
        
        HttpHeaders headers = new HttpHeaders();
        this.mvc.perform(get(url)
                            .headers(headers))
                        .andExpect(status().is(200))
                        .andExpect(content().json(getRecordsJsonArray(5)));
        
    }
    
    @Test
    public void testCount() throws Exception {
        
        String url = MetadataCatalogWebserviceRequest.builder()
                .withQuery("property==value")
                .withSort(Arrays.asList("Property DESC"))
                .withLimit(10)
                .withGroupFunctions("first(Property)")
                .toUrl("/catalogs/DATAKOW_CATALOG/count");
        
        HttpHeaders headers = new HttpHeaders();
        this.mvc.perform(get(url)
                            .headers(headers))
                        .andExpect(status().is(200))
                        .andExpect(content().json("{\"Num-Records\":" + 4 + "}"));
        
    }
    
    @Test
    public void testDistinct() throws Exception {
        
        String url = MetadataCatalogWebserviceRequest.builder()
                .withQuery("property==value")
                .withSort(Arrays.asList("Property DESC"))
                .withLimit(10)
                .withGroupFunctions("first(Property)")
                .withDistinct("Property")
                .toUrl("/catalogs/DATAKOW_CATALOG/distinct");
        
        HttpHeaders headers = new HttpHeaders();
        this.mvc.perform(get(url)
                            .headers(headers))
                        .andExpect(status().is(200))
                        .andExpect(content().json("[\"one\",\"two\",\"three\"]"));
        
    }

    @Test
    public void testFindById() throws Exception {
        
        HttpHeaders headers = new HttpHeaders();
        this.mvc.perform(get("/catalogs/DATAKOW_CATALOG/records/" + testConfig.mockRecord().getStorage().getId())
                            .headers(headers))
                        .andExpect(status().is(200))
                        .andExpect(content().json(testConfig.mockRecord().toJson()));
    }

    @Test
    public void testJsonPatchById() throws Exception{
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json-patch+json");
        DotNotationList<JsonPatchOperation> patches = new DotNotationList<>();
        patches.add(JsonPatchOperation.add("/Doc/property", "123"));
        this.mvc.perform(patch("/catalogs/DATAKOW_CATALOG/records/" + testConfig.mockRecord().getStorage().getId())
                            .headers(headers)
                            .content(patches.toJson().getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Location", "/catalogs/DATAKOW_CATALOG/records/" + testConfig.mockRecord().getStorage().getId()))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(content().json("{\"id\":\"" + testConfig.mockRecord().getStorage().getId() + "\"}"));
    }

    @Test
    public void testJsonPatchByQueryLimitOneUpsert() throws Exception{
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json-patch+json");
        DotNotationList<JsonPatchOperation> patches = new DotNotationList<>();
        patches.add(JsonPatchOperation.add("/Doc/property", "123"));
        List<BulkResult> expected = new ArrayList<>();
        expected.add(testConfig.getBulkResultCreate().get(0));
        expected.get(0).remove("Operation.Correlation-Id");
        String response = new DotNotationList(expected).toJson();
        this.mvc.perform(patch("/catalogs/DATAKOW_CATALOG/records?s=things==stuff&sort=prop DESC&limit=1&upsert=true")
                            .headers(headers)
                            .content(patches.toJson().getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(content().json(response));
    }
    
    @Test
    public void testJsonPatchByQueryLimitOne() throws Exception{
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json-patch+json");
        DotNotationList<JsonPatchOperation> patches = new DotNotationList<>();
        patches.add(JsonPatchOperation.add("/Doc/property", "123"));
        List<BulkResult> expected = new ArrayList<>();
        expected.add(testConfig.getBulkResultUpdateOne().get(0));
        expected.get(0).remove("Operation.Correlation-Id");
        String response = new DotNotationList(expected).toJson();
        this.mvc.perform(patch("/catalogs/DATAKOW_CATALOG/records?s=things==stuff&sort=prop DESC&limit=1&upsert=false")
                            .headers(headers)
                            .content(patches.toJson().getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(content().json(response));
    }
    
    @Test
    public void testJsonPatchByQueryLimitFive() throws Exception{
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/json-patch+json");
        DotNotationList<JsonPatchOperation> patches = new DotNotationList<>();
        patches.add(JsonPatchOperation.add("/Doc/property", "123"));
        List<BulkResult> expected = new ArrayList<>();
        expected.addAll(testConfig.getBulkResultCreate());
        for(BulkResult res : expected){
            res.remove("Operation.Correlation-Id");
        }
        
        String response = new DotNotationList(expected).toJson();
        this.mvc.perform(patch("/catalogs/DATAKOW_CATALOG/records?s=things==stuff&sort=prop DESC&limit=5&upsert=true")
                            .headers(headers)
                            .content(patches.toJson().getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE));
                        //.andExpect(content().json(response));
    }
    
    @Test
    public void testMergePatchById() throws Exception{
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/merge-patch+json");
        DotNotationMap patch = new DotNotationMap();
        patch.setProperty("Doc.someProperty", "someValue");
        patch.setProperty("Storage.tags", Arrays.asList("One", "two"));
        this.mvc.perform(patch("/catalogs/DATAKOW_CATALOG/records/" + testConfig.mockRecord().getStorage().getId())
                            .headers(headers)
                            .content(patch.toJson().getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Location", "/catalogs/DATAKOW_CATALOG/records/" + testConfig.mockRecord().getStorage().getId()))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(content().json("{\"id\":\"" + testConfig.mockRecord().getStorage().getId() + "\"}"));
    }
    
    @Test
    public void testMergePatchByQueryNotMultiSort() throws Exception{
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/merge-patch+json");
        DotNotationMap patch = new DotNotationMap();
        patch.setProperty("Doc.someProperty", "someValue");
        patch.setProperty("Storage.tags", Arrays.asList("One", "two"));
        this.mvc.perform(patch("/catalogs/DATAKOW_CATALOG/records?s=somepro==someValue&&sort=Prop asc&multi=false")
                            .headers(headers)
                            .content(patch.toJson().getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(header().string("Num-Updated", "1"))
                        .andExpect(content().json("{\"numUpdated\":1}"));
    }
    
    @Test
    public void testMergePatchByQueryMulti() throws Exception{
        HttpHeaders headers = new HttpHeaders();
        headers.add("Content-Type", "application/merge-patch+json");
        DotNotationMap patch = new DotNotationMap();
        patch.setProperty("Doc.someProperty", "someValue");
        patch.setProperty("Storage.tags", Arrays.asList("One", "two"));
        this.mvc.perform(patch("/catalogs/DATAKOW_CATALOG/records?s=somepro==someValue&&multi=true")
                            .headers(headers)
                            .content(patch.toJson().getBytes()))
                        .andExpect(status().is(200))
                        .andExpect(header().string("Content-Type", MediaType.APPLICATION_JSON_VALUE))
                        .andExpect(header().string("Num-Updated", "1"))
                        .andExpect(content().json("{\"numUpdated\":1}"));
    }
    
    @Test
    public void testParseIdentities() {
        
        CatalogIdentityCollection coll = controller.parseIdentities(Arrays.asList("DATAKOW_CATALOG ;1234 ", " DATAKOW_OTHER;   789"));
        assertEquals(coll.get(0).getCatalogIdentifier(), "DATAKOW_CATALOG");
        assertEquals(coll.get(0).getRecordIdentifier(), "1234");
        assertEquals(coll.get(1).getCatalogIdentifier(), "DATAKOW_OTHER");
        assertEquals(coll.get(1).getRecordIdentifier(), "789");
        
        coll = controller.parseIdentities(Arrays.asList("DATAKOW_CATALOG ; 1234 "));
        assertEquals(coll.get(0).getCatalogIdentifier(), "DATAKOW_CATALOG");
        assertEquals(coll.get(0).getRecordIdentifier(), "1234");
    }

    @Test
    public void testBuildFinalMetadataCatalogRecord() throws JsonProcessingException {
        MetadataCatalogRecord mockRecord = testConfig.mockRecord();
        MetadataCatalogRecord record = controller.buildFinalMetadataCatalogRecord(
                DotNotationMap.fromObject(mockRecord.getStorage()), 
                mockRecord.getDocument(), 
                UUID.randomUUID().toString(), 
                "public", 
                null, 
                null, 
                "awc");
        record.getStorage().setPublishDate(mockRecord.getStorage().getPublishDate());
        assertEquals(mockRecord, record);
    }
    
    @Test
    public void testBuildFinalMetadataCatalogRecordNewTags() throws JsonProcessingException {
        MetadataCatalogRecord mockRecord = testConfig.mockRecord();
        mockRecord.getStorage().setTags(null);
        MetadataCatalogRecord record = controller.buildFinalMetadataCatalogRecord(
                DotNotationMap.fromObject(mockRecord.getStorage()), 
                mockRecord.getDocument(), 
                UUID.randomUUID().toString(), 
                "public", 
                Arrays.asList("Tag1", "Tag2"), 
                null, 
                "awc");
        record.getStorage().setPublishDate(mockRecord.getStorage().getPublishDate());
        mockRecord.getStorage().setTags(Arrays.asList("Tag1", "Tag2"));
        assertEquals(mockRecord, record);
    }

    @Test
    public void testBuildFinalMetadataCatalogRecordYouCantChangePublisher() throws JsonProcessingException {
        MetadataCatalogRecord mockRecord = testConfig.mockRecord();
        mockRecord.getStorage().setPublisher("FRANK");
        MetadataCatalogRecord record = controller.buildFinalMetadataCatalogRecord(
                DotNotationMap.fromObject(mockRecord.getStorage()), 
                mockRecord.getDocument(), 
                UUID.randomUUID().toString(), 
                "public", 
                null, 
                null, 
                "awc");
        //Even though the record said FRANK it must be awc
        mockRecord.getStorage().setPublisher("awc");
        record.getStorage().setPublishDate(mockRecord.getStorage().getPublishDate());
        assertEquals(mockRecord, record);
    }
    
    @Test
    public void testParseInputStreamOneRecord() throws Exception {
        ByteArrayInputStream is = new ByteArrayInputStream(testConfig.mockRecord().toJson().getBytes());
        ParsedInputStream parsedStream = controller.parseInputStream(is, testConfig.mockRecord().getStorage().getId(), "weather", null, null, "awc");
        MetadataCatalogRecord record = testConfig.mockRecord();
        record.getStorage().setPublisher("awc");
        record.getStorage().setRealm("weather");
        record.getStorage().setPublishDate(parsedStream.getRecord().getStorage().getPublishDate());
        assertEquals(record.toJson(), parsedStream.getRecord().toJson());
    }
    
    @Test
    public void testParseInputStreamTwoRecords() throws Exception {
        ByteArrayInputStream is = new ByteArrayInputStream(getRecordsJsonArray(5).getBytes());
        ParsedInputStream parsedStream = controller.parseInputStream(is, testConfig.mockRecord().getStorage().getId(), "secret", null, new CatalogIdentityCollection(testConfig.mockObjectCatalogIdentity()), "bob");
        String twoRecordsString = IOUtils.toString(parsedStream.getInputStream());
        List<MetadataCatalogRecord> records = MetadataCatalogRecord.fromJsonArray(twoRecordsString);
        int index = 0;
        for(MetadataCatalogRecord record : records){
            MetadataCatalogRecord mockRecord = testConfig.mockRecord(index);
            record.getStorage().setPublishDate(mockRecord.getStorage().getPublishDate());
            assertEquals(mockRecord.toJson(), record.toJson());
            index++;
        }
    }
    
    @Test
    public void testParseInputStreamBadBody() throws Exception{
        ByteArrayInputStream is = new ByteArrayInputStream("".getBytes());
        ParsedInputStream parsedStream = controller.parseInputStream(is, testConfig.mockRecord().getStorage().getId(), "secret", null, new CatalogIdentityCollection(testConfig.mockObjectCatalogIdentity()), "bob");
        assertNull(parsedStream);
        
    }
    
    private String getRecordsJsonArray(int count) throws JsonProcessingException{
        List<MetadataCatalogRecord> records = testConfig.mockRecords(count);
        return new DotNotationList(records).toJson();
    }
    
    private String getDocumentsJsonArray(int count) throws JsonProcessingException{
        List<MetadataCatalogRecord> records = testConfig.mockRecords(count);
        return new DotNotationList(records.stream().map(r -> r.getDocument()).collect(Collectors.toList())).toJson();
    }
    
    private String getBulkResultUpsertString() throws JsonProcessingException{
        return new DotNotationList(testConfig.getBulkResultUpsert()).toJson();
    }
    
    private String getBulkResultCreateString() throws JsonProcessingException{
        return new DotNotationList(testConfig.getBulkResultCreate()).toJson();
    }
}
