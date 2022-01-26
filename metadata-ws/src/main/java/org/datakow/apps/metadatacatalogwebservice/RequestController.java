package org.datakow.apps.metadatacatalogwebservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.client.DistinctIterable;
import com.mongodb.client.MongoIterable;
import com.mongodb.client.result.UpdateResult;

import org.datakow.apps.metadatacatalogwebservice.exception.CatalogDoesNotExistException;
import org.datakow.apps.metadatacatalogwebservice.exception.InvalidRequestBodyException;
import org.datakow.apps.metadatacatalogwebservice.exception.InvalidRequestParameterException;
import org.datakow.apps.metadatacatalogwebservice.exception.RecordNotFoundException;
import org.datakow.catalogs.metadata.database.MetadataDataCoherence;
import org.datakow.catalogs.metadata.database.MongoDBMetadataCatalogDao;
import org.datakow.catalogs.metadata.database.MongoRecordStream;
import org.datakow.catalogs.metadata.jsonpatch.JsonPatchOperation;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.DotNotationList;
import org.datakow.core.components.DotNotationMap;
import org.datakow.core.components.JsonInputStreamToIterator;
import org.datakow.core.components.DatakowObjectMapper;
import org.datakow.messaging.events.CatalogEventsSenderClient;
import org.datakow.messaging.events.events.RecordAssociationEvent;
import org.datakow.messaging.events.events.RecordEvent;

import java.io.IOException;
import java.io.InputStream;
import java.io.SequenceInputStream;
import java.net.URI;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.datakow.catalogs.metadata.BulkResult;
import org.datakow.catalogs.metadata.Catalog;
import org.datakow.catalogs.metadata.CatalogRegistry;
import org.datakow.catalogs.metadata.MetadataCatalogRecord;
import org.datakow.catalogs.metadata.MetadataCatalogRecordStorage;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author kevin.off
 */
@CrossOrigin
@RestController
@RequestMapping(value = {"/"})
public class RequestController {
    
    @Autowired
    CatalogRegistry catalogRegistry;

    @Autowired
    MongoDBMetadataCatalogDao metadataCatalogDao;

    @Autowired
    CatalogEventsSenderClient eventsSenderClient;

    private final Logger logger;
    
    public RequestController() {
        logger = LogManager.getLogger(this.getClass());
    }

    @RequestMapping(value="", method = {RequestMethod.GET, RequestMethod.HEAD}, produces = {MediaType.TEXT_PLAIN_VALUE})
    public ResponseEntity<String> defaultPage(){
        return new ResponseEntity<String>(HttpStatus.OK);
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records/{id}", method=RequestMethod.GET, 
            produces = MediaType.APPLICATION_JSON_VALUE)
    public void findById(
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @PathVariable("id") String recordIdentifier, ServletOutputStream out,
            @RequestParam(value = "properties", required = false) List<String> properties,
            @RequestParam(value = "dataCoherence", required = false, defaultValue = "available") String dataCoherence,
            HttpServletResponse response, 
            @RequestHeader Map<String, String> headers) throws IOException {

        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        ThreadContext.put("Record-Identifier", recordIdentifier);
        logger.info("Received findById : {} : {}", catalogIdentifier, recordIdentifier);
        
        MetadataDataCoherence coherence = MetadataDataCoherence.fromString(dataCoherence);
        
        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();
        
        logger.debug("About to get record " + recordIdentifier + " from the " + catalogIdentifier + " catalog");
        MetadataCatalogRecord result = metadataCatalogDao.getById(collectionName, recordIdentifier, properties, coherence);
        logger.debug("Done getting record " + recordIdentifier + " from the " + catalogIdentifier + " catalog:");
        
        if (result != null) {
            String jsonRecord = result.toJson();
            Charset utf8 = StandardCharsets.UTF_8;
            byte[] bytes = jsonRecord.getBytes(utf8);
            response.setContentLength(bytes.length);
            response.setCharacterEncoding(utf8.name().toLowerCase());
            response.setContentType(MediaType.APPLICATION_JSON_VALUE);
            out.write(bytes);
        } else {
            throw new RecordNotFoundException(new CatalogIdentity(catalogIdentifier, recordIdentifier));
        }
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records", 
            method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public void findByQuery(HttpServletResponse response, 
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @RequestParam(value="properties", required = false) List<String> projectionList,
            @RequestParam(value = "sort", required = false, defaultValue = "") String sort, 
            @RequestParam(value = "limit", required = false, defaultValue = "-1") int limit, 
            @RequestParam(value = "s", required = false) String fiql, 
            @RequestParam(value = "count", required = false, defaultValue = "false") boolean count,
            @RequestParam(value = "distinct", required = false, defaultValue = "") String distinct,
            @RequestParam(value = "dataCoherence", required = false, defaultValue = "available") String coherence,
            @RequestParam(value = "pipeline", required = false) String pipeline,
            @RequestHeader Map<String, String> headers) throws IOException{
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        logger.info("Received findByQuery : {} : {}", catalogIdentifier, fiql);
        
        MetadataDataCoherence dataCoherence = MetadataDataCoherence.fromString(coherence);
        
        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();
        
        if (StringUtils.hasText(distinct)){
            MongoIterable<Object> distinctValues = metadataCatalogDao.distinct(collectionName, distinct, fiql, dataCoherence, Object.class);
            DatakowObjectMapper.getObjectMapper().writeValue(response.getOutputStream(), distinctValues);
            return;
        }
        
        if (count == true){
            long numRecords = metadataCatalogDao.count(collectionName, fiql, limit, dataCoherence);
            response.getOutputStream().print("{\"Num-Records\":" + numRecords + "}");
            return;
        }
        
        MongoRecordStream<MetadataCatalogRecord> results;
        
        if (StringUtils.hasText(pipeline)){
            results = metadataCatalogDao.aggregate(collectionName, pipeline, dataCoherence);
        }else if (!catalogIdentifier.startsWith("DATAKOW_") || StringUtils.hasText(fiql) || limit > 0){
            logger.debug("About to execute query: " + fiql);
            results = metadataCatalogDao.getByQuery(collectionName, fiql, sort, limit, projectionList, dataCoherence);
            logger.debug("Done executing query: " + fiql);
        }else{
            throw new InvalidRequestParameterException("s or limit", "You must provide a FIQL query or a limit");
        }
        
        results.hasNext();
        response.setStatus(HttpServletResponse.SC_OK);
        MetadataCatalogRecord record;
        String recordString;
        response.setContentType("application/json");
        response.getOutputStream().write("[".getBytes("UTF-8"));
        if (results.hasNext()){
            while(results.hasNext()){

                record = results.next();
                recordString = record.toJson();

                response.getOutputStream().write(recordString.getBytes("UTF-8"));
                if (results.hasNext()){
                    response.getOutputStream().write(",".getBytes("UTF-8"));
                }
            } 
        }
        response.getOutputStream().write("]".getBytes("UTF-8"));
        results.close();
    }
    
    
    
    @RequestMapping(
        value = "/catalogs/{catalogIdentifier}/count", 
        method=RequestMethod.GET, 
        produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> count(
            @PathVariable("catalogIdentifier") String catalogIdentifier,
            @RequestParam(value = "s", required = false) String fiql,
            @RequestParam(value = "limit", required = false, defaultValue = "0") int limit,
            @RequestParam(value = "dataCoherence", required = false, defaultValue = "available") String coherence) throws JsonProcessingException{
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        logger.info("Received count : {} : {}", catalogIdentifier, fiql);
        
        MetadataDataCoherence dataCoherence = MetadataDataCoherence.fromString(coherence);

	    Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();
        
        long count;
        if (limit <= 0){
             count = metadataCatalogDao.count(collectionName, fiql, dataCoherence);
        }else{
            count = metadataCatalogDao.count(collectionName, fiql, limit, dataCoherence);
        }
        HttpHeaders headers = new HttpHeaders();
        headers.add("Num-Records", String.valueOf(count));
        return new ResponseEntity<String>("{\"Num-Records\":" + count + "}", headers, HttpStatus.OK);
        
    }
    
    @RequestMapping(
        value = "/catalogs/{catalogIdentifier}/distinct", 
        method=RequestMethod.GET, 
        produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<List<DotNotationMap>> distinct(
            @PathVariable("catalogIdentifier") String catalogIdentifier,
            @RequestParam(value="s", required = false) String fiql,
            @RequestParam("distinct") String distinct,
            @RequestParam(value = "dataCoherence", required = false, defaultValue = "available") String coherence) throws JsonProcessingException{
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        logger.info("Received distinct : {} : {}", catalogIdentifier, fiql);
        
        MetadataDataCoherence dataCoherence = MetadataDataCoherence.fromString(coherence);

	    Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();
        List<DotNotationMap> results = new ArrayList<>();
        DistinctIterable<DotNotationMap> values = metadataCatalogDao.distinct(collectionName, distinct, fiql, dataCoherence, DotNotationMap.class);
        values.cursor().forEachRemaining(a -> results.add(a));
        return new ResponseEntity<List<DotNotationMap>>(results, HttpStatus.OK);
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records", method = RequestMethod.POST, 
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> create(
            HttpServletRequest request,
            @PathVariable("catalogIdentifier") String catalogIdentifier,
            @RequestHeader(value = "Record-Identifier", required = false, defaultValue = "") String recordIdentifier,
            @RequestHeader(value = "Realm", required = false) String defaultRealm,
            @RequestHeader(value = "Tags", required = false) List<String> defaultTags,
            @RequestHeader(value = "Object-Identities", required = false) List<String> objectIdentities) throws IOException{
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        
        logger.info("Received create : {}", catalogIdentifier);
      
        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();
        String publisher = SecurityContextHolder.getContext().getAuthentication().getName();
        CatalogIdentityCollection defaultObjectIdentities = parseIdentities(objectIdentities);
        ParsedInputStream parsedStream = parseInputStream(request.getInputStream(), recordIdentifier, 
                defaultRealm, defaultTags, defaultObjectIdentities, publisher);
        
        if(parsedStream == null){
            throw new InvalidRequestBodyException("Your request body could not be parsed.");
        }
        if (parsedStream.isAnArray()){
            throw new InvalidRequestBodyException("A POST request with the Operation-Type header missing or with the value of individual requires a single JSON object request body");
        }
        if (parsedStream.getRecord() == null){
            throw new InvalidRequestBodyException("Your request body could not be parsed or is null.");
        }
        
        MetadataCatalogRecord record = parsedStream.getRecord();
        ThreadContext.put("Record-Identifier", record.getStorage().getId());
        logger.info("About to create record " + catalogIdentifier + ":" + record.getStorage().getId());
        metadataCatalogDao.create(collectionName, record);
        CatalogIdentity metadataIdentity = new CatalogIdentity(catalogIdentifier, record.getStorage().getId());
        sendEvents(metadataIdentity, record.getStorage().getObjectIdentities(), "created");

        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setLocation(URI.create("/catalogs/" + catalogIdentifier + "/records/" + record.getStorage().getId()));
        return new ResponseEntity<String>("{\"id\":\"" + record.getStorage().getId() + "\"}", responseHeaders, HttpStatus.CREATED);
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records", method = RequestMethod.POST, 
            produces = MediaType.APPLICATION_JSON_VALUE, headers = {"Operation-Type=bulk"})
    public ResponseEntity<String> createBulk(
            HttpServletRequest request,
            @PathVariable("catalogIdentifier") String catalogIdentifier,
            @RequestHeader(value = "Realm", required=false) String realm,
            @RequestHeader(value = "Tags", required=false) List<String> tags,
            @RequestHeader(value = "Object-Identities", required=false) List<String> identities) throws JsonProcessingException, IOException{
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        logger.info("Received create bulk : {}", catalogIdentifier);
        
        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();
        CatalogIdentityCollection objectIdentities = parseIdentities(identities);
        String publisher = SecurityContextHolder.getContext().getAuthentication().getName();
        ParsedInputStream parsedStream = parseInputStream(request.getInputStream(), null, 
                realm, tags, objectIdentities, publisher);
        
        if(parsedStream == null){
            throw new InvalidRequestBodyException("Your request body could not be parsed.");
        }
        if (!parsedStream.isAnArray()){
            throw new InvalidRequestBodyException("A POST request with the Operation-Type=bulk requires an array of JSON objects in the request body.");
        }

        List<BulkResult> ids = metadataCatalogDao.createBulk(
                collectionName, publisher, realm, tags, parsedStream.getInputStream(), objectIdentities);

        String metadataRecordIdentifier;
        for(BulkResult idMap : ids){

            metadataRecordIdentifier = idMap.getRecordIdentifier();
            if (idMap.getSuccess() && idMap.getActionTaken().equals("created") && metadataRecordIdentifier != null){
                CatalogIdentity metadataIdentity = new CatalogIdentity(catalogIdentifier, metadataRecordIdentifier);
                sendEvents(metadataIdentity, objectIdentities, "created");
            }
        }

        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowDateAwareObjectMapper();
        HttpHeaders responseHeaders = new HttpHeaders();
        return new ResponseEntity<String>(mapper.writeValueAsString(ids), responseHeaders, HttpStatus.OK);
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records/{id}", method = RequestMethod.PUT, 
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateById(
            HttpServletRequest request,
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @PathVariable("id") String recordIdentifier, 
            @RequestHeader(value = "Realm", required = false) String defaultRealm,
            @RequestHeader(value = "Tags", required = false) List<String> defaultTags,
            @RequestHeader(value = "Object-Identities", required = false) List<String> objectIdentities) throws IOException{
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        ThreadContext.put("Record-Identifier", recordIdentifier);
      
        logger.info("Received update one by id : {} : {}", catalogIdentifier, recordIdentifier);

        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();
        String publisher = SecurityContextHolder.getContext().getAuthentication().getName();
        CatalogIdentityCollection defaultObjectIdentities = parseIdentities(objectIdentities);

        ParsedInputStream parsedStream = parseInputStream(request.getInputStream(), recordIdentifier, 
                defaultRealm, defaultTags, defaultObjectIdentities, publisher);
        
        if(parsedStream == null){
            throw new InvalidRequestBodyException("Your request body could not be parsed.");
        }
        if (parsedStream.isAnArray()){
            throw new InvalidRequestBodyException("A PUT request to update one record by query requires a single JSON object request body");
        }
        if (parsedStream.getRecord() == null){
            throw new InvalidRequestBodyException("Your request body could not be parsed or is null.");
        }
        
        MetadataCatalogRecord record = parsedStream.getRecord();
        
        //You cannot change the ID of the record;
        record.getStorage().setId(recordIdentifier);
        
        UpdateResult result = metadataCatalogDao.updateByQuery(collectionName, "Storage.Record-Identifier==" + recordIdentifier, null, record, publisher, false, false);
        
        if (result.getModifiedCount() == 0){
            throw new RecordNotFoundException(new CatalogIdentity(catalogIdentifier, recordIdentifier));
        }else{
            HttpHeaders responseHeaders = new HttpHeaders();
            responseHeaders.setLocation(URI.create("/catalogs/" + catalogIdentifier + "/records/" + recordIdentifier));
            return new ResponseEntity<String>("{\"id\":\"" + recordIdentifier + "\"}", responseHeaders, HttpStatus.OK);
        }
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records", method = RequestMethod.PUT, 
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> updateOneByQuery(
            HttpServletRequest request,
            @PathVariable("catalogIdentifier") String catalogIdentifier,
            @RequestParam("s") String fiql,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestHeader(value = "Realm", required = false) String defaultRealm,
            @RequestHeader(value = "Tags", required = false) List<String> defaultTags,
            @RequestHeader(value = "Object-Identities", required = false) List<String> objectIdentities) throws IOException{
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        logger.info("Received update one by query : {}", catalogIdentifier);

        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();
        String publisher = SecurityContextHolder.getContext().getAuthentication().getName();
        CatalogIdentityCollection defaultObjectIdentities = parseIdentities(objectIdentities);
  
        ParsedInputStream parsedStream = parseInputStream(request.getInputStream(), 
                null, defaultRealm, defaultTags, defaultObjectIdentities, publisher);
        
        if(parsedStream == null){
            throw new InvalidRequestBodyException("Your request body could not be parsed.");
        }
        if (parsedStream.isAnArray()){
            throw new InvalidRequestBodyException("A PUT request with the Operation-Type header missing or with the value of individual requires a single JSON object request body");
        }
        if (parsedStream.getRecord() == null){
            throw new InvalidRequestBodyException("Your request body could not be parsed or is null.");
        }
        
        MetadataCatalogRecord record = parsedStream.getRecord();

        UpdateResult result = metadataCatalogDao.updateByQuery(collectionName, fiql, sort, record, publisher, false, false);
        if (result.getModifiedCount() == 0){
            throw new RecordNotFoundException(new CatalogIdentity(catalogIdentifier, record.getStorage().getId()));
        }else{
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
            return new ResponseEntity<String>(headers, HttpStatus.OK);
        }
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records", method = RequestMethod.PUT, 
            produces = MediaType.APPLICATION_JSON_VALUE, headers = {"Operation-Type=bulk"})
    public ResponseEntity<String> updateBulkByParameterizedFilter(
            HttpServletRequest request,
            @PathVariable("catalogIdentifier") String catalogIdentifier,
            @RequestParam(value = "filter", required=false, defaultValue = "Storage.Record-Identifier=={Storage.Record-Identifier}") String filter,
            @RequestParam(value = "multi", required = false, defaultValue = "false") boolean multi,
            @RequestHeader(value = "Realm", required=false) String realm,
            @RequestHeader(value = "Tags", required=false) List<String> tags,
            @RequestHeader(value = "Object-Identities", required=false) List<String> identities) throws JsonProcessingException, IOException{
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        logger.info("Received Update Bulk : {}", catalogIdentifier);
        
        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();
        CatalogIdentityCollection objectIdentities = parseIdentities(identities);
        String publisher = SecurityContextHolder.getContext().getAuthentication().getName();
        ParsedInputStream parsedStream = parseInputStream(request.getInputStream(), null, 
                realm, tags, objectIdentities, publisher);
        
        if(parsedStream == null){
            throw new InvalidRequestBodyException("Your request body could not be parsed.");
        }
        if (!parsedStream.isAnArray()){
            throw new InvalidRequestBodyException("A PUT request with the Operation-Type=bulk requires an array of JSON objects in the request body.");
        }

        List<BulkResult> ids = metadataCatalogDao.updateBulkByParameterizedFilter(
                collectionName, publisher, realm, tags, filter, parsedStream.getInputStream(), 
                objectIdentities, false, multi);

        String metadataRecordIdentifier;
        for(BulkResult idMap : ids){

            metadataRecordIdentifier = idMap.getRecordIdentifier();
            if (idMap.getSuccess() && idMap.getActionTaken().equals("created") && metadataRecordIdentifier != null){
                CatalogIdentity metadataIdentity = new CatalogIdentity(catalogIdentifier, metadataRecordIdentifier);
                sendEvents(metadataIdentity, objectIdentities, "created");
            }
        }

        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowDateAwareObjectMapper();
        HttpHeaders responseHeaders = new HttpHeaders();
        return new ResponseEntity<String>(mapper.writeValueAsString(ids), responseHeaders, HttpStatus.OK);
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records/{id}", method=RequestMethod.PATCH, 
            produces = MediaType.APPLICATION_JSON_VALUE, consumes = {"application/json-patch+json"})
    public ResponseEntity<String> jsonPatchById(
            HttpServletRequest request,
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @PathVariable("id") String recordIdentifier) throws IOException {
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        ThreadContext.put("Record-Identifier", recordIdentifier);
      
        logger.info("Received jsonPatchById : {} : {}", catalogIdentifier, recordIdentifier);

        List<BulkResult> results = performPatchByQuery(catalogIdentifier, "Storage.Record-Identifier==" + recordIdentifier, null, 1, false, request.getInputStream());
        
        if (results.isEmpty()){
            throw new RecordNotFoundException(new CatalogIdentity(catalogIdentifier, recordIdentifier));
        }else if(results.get(0).getSuccess() && results.get(0).getActionTaken().equals("updated")){
            HttpHeaders responseHeaders = new HttpHeaders();
            responseHeaders.setLocation(URI.create("/catalogs/" + catalogIdentifier + "/records/" + recordIdentifier));
            return new ResponseEntity<String>("{\"id\":\"" + recordIdentifier + "\"}", responseHeaders, HttpStatus.OK);
        }else{
            logger.error("Action-Taken: " + results.get(0).getActionTaken() + ", Error-Message: " + results.get(0).getErrorMessage());
            return new ResponseEntity<String>(
                    CustomExceptionHandler.buildErrorMessage(
                            "UNEXPECTED_REPONSE", 
                            "Action-Taken: " + results.get(0).getActionTaken() + ", Error-Message: " + results.get(0).getErrorMessage()),
            HttpStatus.INTERNAL_SERVER_ERROR);
        }
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records", method=RequestMethod.PATCH, 
            produces = MediaType.APPLICATION_JSON_VALUE, consumes = {"application/json-patch+json"})
    public ResponseEntity<String> jsonPatchByQuery(
            HttpServletRequest request,
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @RequestParam("s") String fiql,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "limit", required = false, defaultValue = "-1") int limit,
            @RequestParam(value = "upsert", required = false, defaultValue = "false") boolean upsert) throws IOException {
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
      
        logger.info("Received patchMulti : {} : {}", catalogIdentifier);

        List<BulkResult> ids = performPatchByQuery(catalogIdentifier, fiql, sort, limit, upsert, request.getInputStream());
        
        if (ids.isEmpty()){
            throw new RecordNotFoundException(new CatalogIdentity(catalogIdentifier, catalogIdentifier));
        }
        
        DatakowObjectMapper mapper = DatakowObjectMapper.getDatakowDateAwareObjectMapper();
        HttpHeaders responseHeaders = new HttpHeaders();
        String jsonResponse = mapper.writeValueAsString(ids);
        return new ResponseEntity<String>(jsonResponse, responseHeaders, HttpStatus.OK);
        
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records/{id}", method=RequestMethod.PATCH, 
            produces = MediaType.APPLICATION_JSON_VALUE, consumes = {"application/merge-patch+json"})
    public ResponseEntity<String> mergePatchById(
            HttpServletRequest request,
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @PathVariable("id") String recordIdentifier) throws IOException {
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
      
        logger.info("Received mergePatchByQuery : {} : {}", catalogIdentifier);

        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();

        String publisher = SecurityContextHolder.getContext().getAuthentication().getName();
        
        DotNotationMap mergePatch = DotNotationMap.fromJson(IOUtils.toString(request.getInputStream()));
        
        //Override the Record-Identifier if one exists
        mergePatch.setProperty("Storage.Record-Identifier", recordIdentifier);
        
        UpdateResult result = metadataCatalogDao.mergePatchByQuery(collectionName, "Storage.Record-Identifier==" + recordIdentifier, null, mergePatch, publisher, false, false);
        HttpHeaders responseHeaders = new HttpHeaders();
        if (result.getModifiedCount() > 0){
            responseHeaders.setLocation(URI.create("/catalogs/" + catalogIdentifier + "/records/" + recordIdentifier));
            return new ResponseEntity<String>("{\"id\":\"" + recordIdentifier + "\"}", responseHeaders, HttpStatus.OK);
        }else{
            throw new RecordNotFoundException(new CatalogIdentity(catalogIdentifier, recordIdentifier));
        }
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records", method=RequestMethod.PATCH, 
            produces = MediaType.APPLICATION_JSON_VALUE, consumes = {"application/merge-patch+json"})
    public ResponseEntity<String> mergePatchByQuery(
            HttpServletRequest request,
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @RequestParam("s") String fiql,
            @RequestParam(value = "sort", required = false) String sort,
            @RequestParam(value = "multi", required = false, defaultValue = "true") boolean updateMulti,
            @RequestParam(value = "upsert", required = false, defaultValue = "false") boolean upsert) throws IOException {
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
      
        logger.info("Received mergePatchByQuery : {} : {}", catalogIdentifier);

        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();

        String publisher = SecurityContextHolder.getContext().getAuthentication().getName();
        
        DotNotationMap mergePatch = DotNotationMap.fromJson(IOUtils.toString(request.getInputStream()));
        
        UpdateResult result = metadataCatalogDao.mergePatchByQuery(collectionName, fiql, sort, mergePatch, publisher, upsert, updateMulti);
        
        HttpHeaders headers = new HttpHeaders();
        HttpStatus status;
        if (result.getUpsertedId() != null){
            headers.setLocation(URI.create("/catalogs/" + catalogIdentifier + "/records/" + result.getUpsertedId()));
            status = HttpStatus.CREATED;
        }else if(result.getModifiedCount() > 0){
            status = HttpStatus.OK;
        }else{
            status = HttpStatus.NOT_FOUND;
        }
        headers.add("Num-Updated", Long.toString(result.getModifiedCount()));
        return new ResponseEntity<String>("{\"numUpdated\":" + result.getModifiedCount() + "}", headers, status);
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records/{id}", method = RequestMethod.DELETE,
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<String> deleteById(
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @PathVariable("id") String recordIdentifier) throws IOException {

        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        ThreadContext.put("Record-Identifier", recordIdentifier);
        logger.info("Received delete : {} : {}", catalogIdentifier, recordIdentifier);
        
        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();
        
        metadataCatalogDao.deleteById(collectionName, recordIdentifier);
        return new ResponseEntity<>(HttpStatus.OK);
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/records", method = RequestMethod.DELETE, 
            produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<DotNotationMap> deleteByQuery(
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @RequestParam("s") String fiql) throws JsonProcessingException {
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
        logger.info("Received deleteByQuery : {} : {}", catalogIdentifier, fiql);
        
        if (!StringUtils.hasText(fiql)){
            throw new InvalidRequestParameterException(fiql);
        }
        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();
        long deleted = metadataCatalogDao.deleteByQuery(collectionName, fiql).getDeletedCount();
        HttpHeaders headers = new HttpHeaders();
        headers.add("Num-Deleted", String.valueOf(deleted));
        
        return new ResponseEntity<>(headers, HttpStatus.OK);
    }
    
    protected List<BulkResult> performPatchByQuery(String catalogIdentifier, String fiql, String sort, int limit, boolean upsert, InputStream stream) throws JsonProcessingException, IOException{
        
        Catalog catalog = catalogRegistry.getByCatalogIdentifier(catalogIdentifier);
        if (catalog == null){
            throw new CatalogDoesNotExistException(catalogIdentifier);
        }
        String collectionName = catalog.getCollectionName();

        String publisher = SecurityContextHolder.getContext().getAuthentication().getName();
        
        JsonInputStreamToIterator<JsonPatchOperation> opIterator = JsonInputStreamToIterator.makeIterator(stream, JsonPatchOperation.class);
        
        List<JsonPatchOperation> operations = new ArrayList<>();
        while(opIterator.hasNext()){
            JsonPatchOperation op = opIterator.next();
            operations.add(op);
        }
        List<BulkResult> ids;
        if (limit == 1){
            ids = new ArrayList<>();
            UpdateResult result = metadataCatalogDao.patchOneByQuery(collectionName, fiql, sort, operations, publisher, upsert);
            String action;
            if (upsert){
                //If an upsert was requested, a create happened if there is an upsertId
                action = result.getUpsertedId() != null ? "created" : "updated";
            }else{
                //If an update was requested and more than 1 thing was updated
                //If nothing was updated then no action was taken
                if (result.getModifiedCount() > 0){
                    action = "updated";
                }else{
                    return new ArrayList<>();
                }
            }
            String upsertId = null;
            if (result != null && result.getUpsertedId() != null) {
                upsertId = result.getUpsertedId().asString().getValue();
            }
            ids.add(new BulkResult(upsertId, 0, action));
        }else{
            ids = metadataCatalogDao.patchByQuery(collectionName, fiql, sort, limit, operations, publisher, upsert);
        }
        return ids;
    }
    
    protected void sendEvents(CatalogIdentity metadataIdentity, CatalogIdentityCollection objectIdentities, String actionTaken){
        
        RecordEvent metadataCreatedEvent = new RecordEvent(actionTaken, metadataIdentity);

        eventsSenderClient.sendEvent(metadataCreatedEvent, metadataIdentity.getCatalogIdentifier(), metadataIdentity.getRecordIdentifier());

        if (objectIdentities != null){
            for(CatalogIdentity objectIdentity : objectIdentities){
                RecordAssociationEvent associationEvent = new RecordAssociationEvent(actionTaken, objectIdentity, metadataIdentity);
                eventsSenderClient.sendEvent(associationEvent, objectIdentity.getCatalogIdentifier(), metadataIdentity.getCatalogIdentifier());
            }
        }
    }
    
    protected CatalogIdentityCollection parseIdentities(List<String> identities){
        if (identities == null){
            return null;
        }else if (identities.isEmpty()){
            return new CatalogIdentityCollection();
        }else{
            return CatalogIdentityCollection.metadataAssociationFromHttpHeader(String.join(",", identities));
        }
    }
    
    protected MetadataCatalogRecord buildFinalMetadataCatalogRecord(
            DotNotationMap storageObj, DotNotationMap doc, String defaultRecordIdentifier, String defaultRealm, List<String> defaultTags, 
            CatalogIdentityCollection defaultObjectIdentities, String publisher) throws JsonProcessingException{
            
        String storageRecordIdentifier = storageObj != null ? storageObj.getProperty("Record-Identifier") : null;
        String storageRealm = storageObj != null ? storageObj.getProperty("Realm") : null;
        List<String> storageTags = storageObj != null ? storageObj.getProperty("Tags") : null;
        CatalogIdentityCollection storageObjectIdentities = storageObj != null && storageObj.getProperty("Object-Identities") != null 
                ? CatalogIdentityCollection.fromJson(((DotNotationList)storageObj.getProperty("Object-Identities")).toJson()) 
                : null;
        
        String idFinal = !StringUtils.hasText(storageRecordIdentifier) 
                ? (!StringUtils.hasText(defaultRecordIdentifier) ? UUID.randomUUID().toString() : defaultRecordIdentifier)
                : storageRecordIdentifier;
        
        String realmFinal = storageRealm == null ? defaultRealm : storageRealm;
        List<String> tagsFinal = storageTags == null ? defaultTags : storageTags;
        CatalogIdentityCollection objectIdentitiesFinal = storageObjectIdentities == null ? defaultObjectIdentities : storageObjectIdentities;
        
        MetadataCatalogRecordStorage storage = new MetadataCatalogRecordStorage();
        storage.setId(idFinal);
        storage.setObjectIdentities(objectIdentitiesFinal);
        storage.setPublishDate(new Date());
        storage.setPublisher(publisher);
        storage.setRealm(realmFinal);
        storage.setTags(tagsFinal);
        MetadataCatalogRecord record = new MetadataCatalogRecord();
        record.setStorage(storage);
        record.setDocument(doc);
        return record;
    }
    
    protected ParsedInputStream parseInputStream(
            InputStream stream, String recordIdentifier, String defaultRealm, 
            List<String> defaultTags, CatalogIdentityCollection defaultObjectIdentities, 
            String publisher) 
            throws IOException{
        
        byte[] buffer = new byte[1];
        StringBuilder builder = new StringBuilder();
        String firstChar = null;
        while(stream.read(buffer) != -1){
            String someChar = new String(buffer);
            builder.append(someChar);
            if (someChar.equals("[") || someChar.equals("{")){
                firstChar = someChar;
                break;
            }
        }
        if (firstChar == null || (!firstChar.equals("[") && !firstChar.equals("{"))){
            return null;
        }
        InputStream streamOne = IOUtils.toInputStream(firstChar);
        SequenceInputStream combinedStream = new SequenceInputStream(streamOne, stream);
        
        if (firstChar.equals("[")){
            return new ParsedInputStream(combinedStream);
        }else{
            //Could be a Doc or a Record
            String docOrRecordString = IOUtils.toString(combinedStream);
            IOUtils.closeQuietly(combinedStream);
            DotNotationMap docOrRecord = DotNotationMap.fromJson(docOrRecordString);

            MetadataCatalogRecord record;
            if (docOrRecord.containsKey("Doc") && docOrRecord.containsKey("Storage")){
                //It is a Record
                record = buildFinalMetadataCatalogRecord(
                        docOrRecord.getProperty("Storage"), docOrRecord.getProperty("Doc"), 
                        recordIdentifier, defaultRealm, defaultTags, defaultObjectIdentities, 
                        publisher);

            }else{
                //It is a Doc
                record = buildFinalMetadataCatalogRecord(
                        null, docOrRecord, recordIdentifier, defaultRealm, defaultTags, defaultObjectIdentities, 
                        publisher);
            }
            return new ParsedInputStream(record);
        } 
    }
    
    protected class ParsedInputStream{
        
        private InputStream is;
        private MetadataCatalogRecord record;
        private boolean anArray;

        public ParsedInputStream(InputStream is) {
            this.is = is;
            this.anArray = true;
        }
        
        public ParsedInputStream(MetadataCatalogRecord record) {
            this.record = record;
            this.anArray = false;
        }

        public InputStream getInputStream() {
            return is;
        }

        public void setInputStream(InputStream is) {
            this.is = is;
        }

        public MetadataCatalogRecord getRecord() {
            return record;
        }

        public void setRecord(MetadataCatalogRecord record) {
            this.record = record;
        }

        public boolean isAnArray() {
            return anArray;
        }

        public void setAnArray(boolean anArray) {
            this.anArray = anArray;
        }
        
    }
}
