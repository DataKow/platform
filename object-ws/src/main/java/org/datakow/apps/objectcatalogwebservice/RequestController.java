package org.datakow.apps.objectcatalogwebservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.client.result.UpdateResult;

import org.datakow.apps.objectcatalogwebservice.exception.InvalidRequestParameterException;
import org.datakow.apps.objectcatalogwebservice.exception.MissingContentTypeException;
import org.datakow.apps.objectcatalogwebservice.exception.SourceRecordNotFoundException;
import org.datakow.apps.objectcatalogwebservice.exception.UnauthorizedException;
import org.datakow.core.components.Base64OutputStream;
import org.datakow.catalogs.object.database.MongoDBObjectCatalogDao;
import org.datakow.catalogs.object.database.MongoRecordPropertyStream;
import org.datakow.catalogs.object.database.ObjectDataCoherence;
import org.datakow.catalogs.object.webservice.configuration.ObjectCatalogWebServiceClientConfigurationProperties;
import org.datakow.core.components.CatalogIdentity;
import org.datakow.core.components.CatalogIdentityCollection;
import org.datakow.core.components.DotNotationMap;
import org.datakow.messaging.events.CatalogEventsSenderClient;
import org.datakow.messaging.events.events.EventAction;
import org.datakow.messaging.events.events.RecordAssociationEvent;
import org.datakow.messaging.events.events.RecordEvent;
import org.datakow.security.access.AccessManager;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;
import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import javax.servlet.ServletOutputStream;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.datakow.catalogs.object.ObjectCatalogProperty;
import org.datakow.catalogs.object.ObjectCatalogRecord;
import org.datakow.catalogs.object.ObjectCatalogRecordInput;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.util.StringUtils;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;

/**
 *
 * @author kevin.off
 */
@RestController
@CrossOrigin
@RequestMapping(value = {"/", "/objws/v1"})
public class RequestController {

    @Autowired
    MongoDBObjectCatalogDao objectCatalogDao;

    @Autowired
    CatalogEventsSenderClient eventsSendingClient;
    
    @Autowired
    ObjectCatalogWebServiceClientConfigurationProperties catalogConfig;

    @Autowired
    AccessManager accessManager;
    
    Logger logger = null;

    public RequestController() {
        logger = LogManager.getLogger(this.getClass());
    }

    @RequestMapping(value="/", method = {RequestMethod.GET, RequestMethod.HEAD}, produces = {MediaType.TEXT_PLAIN_VALUE})
    public ResponseEntity defaultPage(){
        return new ResponseEntity(HttpStatus.OK);
    }
    
    @RequestMapping(value = "/catalogs/{catalogName}/objects/{id}", method = RequestMethod.GET)
    public ResponseEntity findById(
            @PathVariable("catalogName") String catalogIdentifier,
            @PathVariable("id") String recordIdentifier, 
            @RequestParam(value = "dataCoherence", required = false, defaultValue = "available") String dataCoherence,
            @RequestHeader Map<String, String> headers) throws JsonProcessingException {

        ThreadContext.put("catalogIdentifier", catalogIdentifier);
        ThreadContext.put("recordIdentifier", recordIdentifier);
        logger.info("Received findById : {} : {}", catalogIdentifier, recordIdentifier);

        ObjectDataCoherence coherence = ObjectDataCoherence.fromString(dataCoherence);
        return findById(recordIdentifier, coherence);
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogName}/objects", method = RequestMethod.GET)
    public void findByQuery(
            HttpServletResponse response,
            @RequestParam(value = "sort", required = false, defaultValue = "") String sort,
            @RequestParam(value = "limit", required = false, defaultValue = "-1") int limit,
            @RequestParam(value = "findOne", required = false, defaultValue = "false") boolean findOne,
            @RequestParam(value = "s", required = false, defaultValue = "") String fiql,
            @PathVariable("catalogName") String catalogIdentifier,
            @RequestParam(value = "collectionFormat", required = false, defaultValue = "json") String collectionFormat,
            @RequestHeader(value = "boundary", required = false, defaultValue = "") String boundary,
            @RequestParam(value = "dataCoherence", required = false, defaultValue = "available") String dataCoherence,
            @RequestHeader Map<String, String> headers) throws JsonProcessingException, IOException {

        ThreadContext.put("catalogIdentifier", catalogIdentifier);
        logger.info("Received findByQuery : {} : {}" , catalogIdentifier, fiql);
        ObjectDataCoherence objCoherence = ObjectDataCoherence.fromString(dataCoherence); 
            
        sort = formatSortString(sort);
        if (findOne) {
            ObjectCatalogRecord record = findOneByQuery(fiql, sort, objCoherence);
            writeOneToResponse(record, response);
        } else {
            if ((fiql != null && !fiql.isEmpty()) || (limit > 0)) {
                MongoRecordPropertyStream stream = findByQuery(fiql, sort, limit, objCoherence);
                try{
                    writeCollectionFormatToResponse(stream, response, collectionFormat, boundary, objCoherence);
                }finally{
                    if (stream != null){
                        stream.close();
                    }
                }
            }else{
                response.setContentType(MediaType.APPLICATION_JSON_VALUE);
                throw new InvalidRequestParameterException("findOne, s, or limit");
            }
        }
    }
    
    @RequestMapping(value = "/catalogs/{catalogName}/objects", method = RequestMethod.POST, produces = {MediaType.APPLICATION_JSON_VALUE})
    public ResponseEntity create(HttpServletRequest request, 
            HttpServletResponse response, 
            @PathVariable("catalogName") String catalogIdentifier,
            @RequestHeader(value = "metadata-catalog-identifiers", required = false) String metadataCatalogIdentifiersString,
            @RequestHeader Map<String, String> headers) throws JsonProcessingException, IOException {


        ThreadContext.put("catalogIdentifier", catalogIdentifier);
        logger.info("Received create : {}", catalogIdentifier);
        //Get headers from request
                
        //TODO verify the content length and MD5 checksum with the MessageDigest class
        String contentType = headers.get("content-type");
        if (contentType == null) contentType = headers.get("Content-Type");
        String realm = headers.get("realm");
        if (realm == null) realm = headers.get("Realm");
        String contentEncoding = headers.get("content-encoding");
        if (contentEncoding == null) contentEncoding = headers.get("Content-Encoding");
        String objectMetadataIdentitiesString = headers.get("metadata-identities");
        if (objectMetadataIdentitiesString == null) objectMetadataIdentitiesString = headers.get("Metadata-Identities");
        String tags = headers.get("Tags");
        if (tags == null) tags = headers.get("Tags");
        if (metadataCatalogIdentifiersString == null) metadataCatalogIdentifiersString = headers.get("Metadata-Catalog-Identifiers");
        
        
        if (!accessManager.canWrite(SecurityContextHolder.getContext().getAuthentication(), realm, null)){
            throw new UnauthorizedException(realm);
        }
        
        //Get the authorization information
        String publisher = SecurityContextHolder.getContext().getAuthentication().getName();

        //Create the input object for this record into the database
        ObjectCatalogRecordInput obj = new ObjectCatalogRecordInput();
        obj.setData(request.getInputStream());

        //Content-Type is required
        if (StringUtils.hasText(contentType)) {
            obj.setContentType(contentType);
        } else {
            throw new MissingContentTypeException();
        }

        //Content-Encoding is NOT required
        if (StringUtils.hasText(contentEncoding)) {
            obj.setContentEncoding(contentEncoding);
        }

        //Parse the type associations
        if (objectMetadataIdentitiesString != null && !objectMetadataIdentitiesString.isEmpty()) {
            CatalogIdentityCollection collection = CatalogIdentityCollection.metadataAssociationFromHttpHeader(objectMetadataIdentitiesString);
            obj.setObjectMetadataIdentities(collection);
        }

        obj.setPublisher(publisher);
        obj.setRealm(realm);
        if (StringUtils.hasText(tags)) {
            obj.setTags(Arrays.asList(tags.split(",")));
        }
        if (StringUtils.hasText(metadataCatalogIdentifiersString)){
            obj.setMetadataCatalogIdentifiers(metadataCatalogIdentifiersString.split(","));
        }
        
        CatalogIdentity savedToDatabase = objectCatalogDao.create(obj);
        
        String recordIdentifier = savedToDatabase.getRecordIdentifier();
        logger.info("Created record " + catalogIdentifier + ":" + recordIdentifier);

        //Create the CatalogIdentity for this object
        CatalogIdentity objectIdentity = new CatalogIdentity(catalogIdentifier, recordIdentifier);

        //Create the object notification to send
        RecordEvent objectEvent = new RecordEvent(EventAction.CREATED);
        objectEvent.setEventId(UUID.randomUUID().toString());
        objectEvent.setCatalogIdentity(objectIdentity);

        //Send the object notification
        logger.debug("About to send Object Notification for object: " + recordIdentifier);
        eventsSendingClient.sendEvent(objectEvent, objectIdentity.getCatalogIdentifier(), objectIdentity.getRecordIdentifier());
        //If the object notification sent ok then start sending notifications for any
        //metadata that is associated

        for (CatalogIdentity metadataAssociation : obj.getObjectMetadataIdentities()) {

            //Create the notification
            RecordAssociationEvent associationEvent = new RecordAssociationEvent(EventAction.CREATED, objectIdentity, metadataAssociation);

            //Send MetadtaAssociationNotification
            logger.debug("About to send Assocation Notification for object " + recordIdentifier);
            eventsSendingClient.sendEvent(associationEvent, objectIdentity.getCatalogIdentifier(), metadataAssociation.getCatalogIdentifier());

        }

        HttpHeaders responseHeaders = new HttpHeaders();
        responseHeaders.setLocation(URI.create("/catalogs/" + catalogIdentifier + "/objects/" + recordIdentifier));
        return new ResponseEntity("{\"id\":\"" + recordIdentifier + "\"}", responseHeaders, HttpStatus.CREATED);

        
    }

    @RequestMapping(value = "/catalogs/{catalogIdentifier}/objects", method=RequestMethod.PATCH, 
            produces = MediaType.APPLICATION_JSON_VALUE, consumes = {"application/merge-patch+json"})
    public ResponseEntity mergePatchByQuery(
            HttpServletRequest request,
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @RequestParam("s") String fiql,
            @RequestParam(value = "multi", required = false, defaultValue = "true") boolean updateMulti) throws IOException {
        
        ThreadContext.put("Catalog-Identifier", catalogIdentifier);
      
        logger.info("Received mergePatchByQuery : {}", catalogIdentifier);

        String publisher = SecurityContextHolder.getContext().getAuthentication().getName();
        
        DotNotationMap mergePatch = DotNotationMap.fromJson(IOUtils.toString(request.getInputStream()));
        
        UpdateResult result = objectCatalogDao.mergePatchByQuery(catalogIdentifier, fiql, mergePatch, updateMulti, publisher);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Num-Updated", Long.toString(result.getModifiedCount()));
        return new ResponseEntity("{\"numUpdated\":" + result.getModifiedCount() + "}", headers, HttpStatus.OK);
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogName}/objects/{id}", method = RequestMethod.DELETE)
    public ResponseEntity delete(
            @PathVariable("id") String recordIdentifier, 
            @PathVariable("catalogName") String catalogIdentifier,
            @RequestHeader Map<String, String> headers) throws IOException {

        ThreadContext.put("catalogIdentifier", catalogIdentifier);
        ThreadContext.put("recordIdentifier", recordIdentifier);
        logger.info("Received delete : {}", recordIdentifier);
        
        Authentication auth = SecurityContextHolder.getContext().getAuthentication();
        
        boolean hasAccess = false;
        String realm = "";
        if (accessManager.canWriteToAnyRealm(auth)){
            hasAccess = true;
        }else{
            ObjectCatalogRecord record = objectCatalogDao.getById(recordIdentifier, ObjectDataCoherence.CONSISTENT);
            IOUtils.closeQuietly(record.getData());
            realm = record.getRealm();
            if (accessManager.canWrite(auth, realm, record.getPublisher())){
                hasAccess = true;
            }
        }
        if (hasAccess){
            objectCatalogDao.deleteById(recordIdentifier);
            logger.info("Deleted record {}", recordIdentifier);
            return new ResponseEntity(HttpStatus.OK);
        }else{
            throw new UnauthorizedException(realm);
        } 
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/objects", method = RequestMethod.DELETE)
    public ResponseEntity deleteByQuery(
            HttpServletResponse response, 
            @PathVariable("catalogIdentifier") String catalogIdentifier,
            @RequestParam("s") String fiql,
            @RequestParam(value = "sort", required = false) String sortString,
            @RequestParam(value = "limit", required = false, defaultValue = "-1") int limit) throws IOException {

        ThreadContext.put("catalogIdentifier", catalogIdentifier);
        logger.info("Received deleteByQuery: fiql:" + fiql + ", limit: " + limit);
        
        if (!StringUtils.hasText(fiql)){
            throw new InvalidRequestParameterException(fiql);
        }
        
        int numDeleted = objectCatalogDao.deleteByQuery(fiql, sortString, limit);
        HttpHeaders headers = new HttpHeaders();
        headers.add("Num-Deleted", String.valueOf(numDeleted));
        return new ResponseEntity(headers, HttpStatus.OK);
        
    }
    
    private ResponseEntity findById(String id, ObjectDataCoherence coherence) throws JsonProcessingException{

        //Retrieve the record by id
        logger.debug("About to retrieve an object by id: " + id + " from Mongo");
        ObjectCatalogRecord result = objectCatalogDao.getById(id, coherence);
        if (result != null) {
            if (accessManager.canRead(
                        SecurityContextHolder.getContext().getAuthentication(), 
                        result.getRealm(), 
                        result.getPublisher())){
                
                logger.debug("Retrieved object by id: " + id + " from Mongo");
                InputStreamResource inStream = new InputStreamResource(result.getData());
                HttpHeaders headers = createResponseHeaders(result);
                headers.set("Content-Length", String.valueOf(result.getContentLength()));
                return new ResponseEntity(inStream, headers, HttpStatus.OK);
            }else{
                throw new UnauthorizedException(result.getRealm());
            }
        } else {
            logger.debug("Object searched by id: " + id + " was not found.");
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
            return new ResponseEntity(
                    CustomExceptionHandler.buildErrorMessage(
                            "NOT_FOUND", 
                            "The record " + id + " could not be found"), 
                    headers, 
                    HttpStatus.NOT_FOUND);
        }
        
    }
    
    private MongoRecordPropertyStream findByQuery(String fiql, String sort, int limit, ObjectDataCoherence coherence){
        
        logger.debug("About to find objects by query " + fiql);
        MongoRecordPropertyStream<String> stream = objectCatalogDao.getByQuery(fiql, sort, limit, coherence);
        return stream;
        
    }
    
    private void writeOneToResponse(ObjectCatalogRecord record, HttpServletResponse response) throws IOException{
        if (record != null) {
            for (Map.Entry<String, List<String>> h : createResponseHeaders(record).entrySet()) {
                response.addHeader(h.getKey(), h.getValue().get(0));
            }
            response.addHeader("Content-Length", String.valueOf(record.getContentLength()));
            response.setStatus(HttpServletResponse.SC_OK);
            try(InputStream stream = record.getData()){
                IOUtils.copy(stream, response.getOutputStream());
            }
        }else{
            response.setStatus(HttpStatus.NOT_FOUND.value());
        }
        
    }
    
    private void writeCollectionFormatToResponse(
            MongoRecordPropertyStream<String> stream, HttpServletResponse response, 
            String collectionFormat, String boundary, ObjectDataCoherence objCoherence) throws JsonProcessingException, IOException{
        switch (collectionFormat) {
            case "multipart_mixed":
                writeMultipartMixedCollectionFormatToResponse(stream, response, boundary, objCoherence);
                break;
            case "json":
                writeJsonCollectionFormatToResponse(stream, response, objCoherence);
                break;
            case "legacy":
                writeLegacyCollectionFormatToResponse(stream, response, objCoherence);
                break;
            default:
                throw new InvalidRequestParameterException("collectionFormat");
        }
    }
    
    private void writeJsonCollectionFormatToResponse(
            MongoRecordPropertyStream<String> stream, HttpServletResponse response, 
            ObjectDataCoherence coherence) throws JsonProcessingException, IOException{
        response.setStatus(HttpServletResponse.SC_OK);    
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        ServletOutputStream out = response.getOutputStream();
        out.print("[");
        
        try(Base64OutputStream base64Out = new Base64OutputStream(out, new byte[0], 0)){

            while(stream != null && stream.hasNext()){
                
                String recordId = stream.next();
                ObjectCatalogRecord record = objectCatalogDao.getById(recordId, coherence);
                
                if (accessManager.canRead(
                        SecurityContextHolder.getContext().getAuthentication(), 
                        record.getRealm(), 
                        record.getPublisher())){
                
                    out.println("{");

                    HttpHeaders headers = createResponseHeaders(record);

                    for(String headerName : headers.keySet()){
                        if (headerName.equals("Metadata-Identities")){
                            out.println("\"" + headerName + "\":" + record.getObjectMetadataIdentities().toJson() + ",");
                        }else if(headerName.equals("Tags")){
                            out.println("\"" + headerName + "\":[" + record.getTags().stream().map(tag -> "\"" + tag + "\"").collect(Collectors.joining(",")) + "],");
                        }else if(headerName.equals("Metadata-Catalog-Identifiers")){
                            out.println("\"" + headerName + "\":[" + record.getMetadataCatalogIdentifiers().stream().map(identifier -> "\"" + identifier + "\"").collect(Collectors.joining(",")) + "],");
                        }else{
                            out.println("\"" + headerName + "\":\"" + headers.getFirst(headerName) + "\",");
                            
                        }
                    }
                    out.print("\"Data\":\"");
                    byte[] bytes = new byte[1024*4];
                    int numBytes;
                    try(InputStream recordStream = record.getData()){
                        while((numBytes = recordStream.read(bytes)) != -1){
                            base64Out.write(bytes, 0, numBytes);
                        }
                    }
                    base64Out.flushBase64();
                    out.print("\"");
                    out.print("}");
                    if (stream.hasNext()){
                        out.println(",");
                    }
                }
            }

            out.print("]");
        }
        
    }
        
    private final Pattern uuidRegex = Pattern.compile("[a-z0-9]{8}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{4}-[a-z0-9]{12}");
    
    private void writeMultipartMixedCollectionFormatToResponse(
            MongoRecordPropertyStream<String> stream, HttpServletResponse response, String boundary,
            ObjectDataCoherence coherence) throws JsonProcessingException, IOException{
        
        response.setStatus(HttpServletResponse.SC_OK);
        if (boundary != null){
            Matcher matcher = uuidRegex.matcher(boundary);
            if (!StringUtils.hasText(boundary) || !matcher.matches()){
                boundary = UUID.randomUUID().toString();
            }
        }
        response.setContentType("multipart/mixed;boundary=" + boundary);
        
        try(ServletOutputStream out = response.getOutputStream()){

            while(stream != null && stream.hasNext()){

                String recordId = stream.next();
                ObjectCatalogRecord record = objectCatalogDao.getById(recordId, coherence);

                if (accessManager.canRead(
                        SecurityContextHolder.getContext().getAuthentication(), 
                        record.getRealm(), 
                        record.getPublisher())){
                    
                    out.println();
                    out.println("--" + boundary);

                    HttpHeaders headers = createResponseHeaders(record);
                    headers.add("Content-Length", String.valueOf(record.getContentLength()));
                    for(String headerName : headers.keySet()){
                        out.println(headerName + ": " + headers.getFirst(headerName));
                    }
                    out.println();
                    try(InputStream recordStream = record.getData()){
                        IOUtils.copy(recordStream, out);
                    }
                    out.flush();
                }
            }

            out.println();
            out.println("--" + boundary + "--");
            out.flush();
        }
    }
    
    private void writeLegacyCollectionFormatToResponse(
            MongoRecordPropertyStream<String> stream, HttpServletResponse response,
            ObjectDataCoherence coherence) 
            throws IOException{
        
        response.setStatus(HttpServletResponse.SC_OK);
        response.setContentType(MediaType.APPLICATION_JSON_VALUE);
        
        ServletOutputStream out = response.getOutputStream();
        
        out.print("[");
        
        if(stream != null && stream.hasNext()){
            
            while (stream.hasNext()) {
                String recordIdentifier = stream.next();
                boolean canRead = accessManager.canReadFromAnyRealm(SecurityContextHolder.getContext().getAuthentication());
                if (!canRead){
                    ObjectCatalogRecord record = objectCatalogDao.getById(recordIdentifier, coherence);
                    IOUtils.closeQuietly(record.getData());
                    canRead = accessManager.canRead(
                        SecurityContextHolder.getContext().getAuthentication(), 
                        record.getRealm(), 
                        record.getPublisher());
                }
                if (canRead){
                    out.print("\"" + recordIdentifier + "\"");
                    if (stream.hasNext()) {
                        out.print(",");
                    }
                }
            }
        }
        out.flush();
        out.print("]");
        
    }
    
    private ObjectCatalogRecord findOneByQuery(String fiql, String sort, ObjectDataCoherence coherence) throws JsonProcessingException{
        
        logger.debug("About to find ONE object by query " + fiql);
        MongoRecordPropertyStream<String> stream = objectCatalogDao.getByQuery(fiql, sort, 1, coherence);
        if (stream.hasNext()) {
            
            String id = stream.next();
            logger.debug("Found ONE object " + id + " by query. About to get object by id. " + fiql);
            stream.close();
            ObjectCatalogRecord record = objectCatalogDao.getById(id, coherence);
            
            if (accessManager.canRead(
                        SecurityContextHolder.getContext().getAuthentication(), 
                        record.getRealm(), 
                        record.getPublisher())){
                return record;
            }else{
                throw new UnauthorizedException(record.getRealm());
            }
            
        } else {
            return null;
        }
        
    }

    private String formatSortString(String orig){
        if (orig != null && !orig.isEmpty()){
            String[] origParts = orig.split(",");
            List<String> newParts = new ArrayList<>();
            for (String origPart : origParts){
                if (!origPart.trim().startsWith(ObjectCatalogProperty.IDENTITIES_PATH)){
                    origPart = ObjectCatalogProperty.IDENTITIES_PATH + "." + origPart.trim();
                }
                newParts.add(origPart);
            }
            return String.join(",", newParts);
        }else{
            return "";
        }
    }
    
    private HttpHeaders createResponseHeaders(ObjectCatalogRecord result) {
        HttpHeaders headers = new HttpHeaders();
        if (result.getId() != null && !result.getId().isEmpty()) {
            headers.add("Record-Identifier", result.getId());
        }
        if (result.getContentType() != null && !result.getContentType().isEmpty()) {
            headers.add("Content-Type", result.getContentType());
        }
        if (result.getContentMD5() != null && !result.getContentMD5().isEmpty()) {
            headers.add("Content-MD5", result.getContentMD5());
        }
        if (result.getObjectMetadataIdentities().size() > 0) {
            headers.add("Metadata-Identities", result.getObjectMetadataIdentities().toHttpHeader());
        }
        if (result.getPublisher() != null && !result.getPublisher().isEmpty()) {
            headers.add("Publisher", result.getPublisher());
        }
        if (result.getContentEncoding() != null && !result.getContentEncoding().isEmpty()) {
            headers.add("Content-Encoding", result.getContentEncoding());
        }
        if (result.getRealm() != null && !result.getRealm().isEmpty()) {
            headers.add("Realm", result.getRealm());
        }
        if (result.getTags() != null && !result.getTags().isEmpty()) {
            headers.add("Tags", String.join(",", result.getTags()));
        }
        if (result.getMetadataCatalogIdentifiers()!= null && !result.getMetadataCatalogIdentifiers().isEmpty()) {
            headers.add("Metadata-Catalog-Identifiers", String.join(",", result.getMetadataCatalogIdentifiers()));
        }
        if (result.getPublishDate() != null) {
            SimpleDateFormat dateFormat = new SimpleDateFormat(
                    "EEE, dd MMM yyyy HH:mm:ss z", Locale.US);
            dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
            headers.add("Publish-Date", dateFormat.format(result.getPublishDate()));
        }
        return headers;
    }
}
