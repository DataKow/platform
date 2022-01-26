package org.datakow.apps.metadatacatalogwebservice;

import org.datakow.catalogs.metadata.database.MongoDBMetadataCatalogManagementDao;
import org.datakow.catalogs.metadata.indexes.MongoIndex;
import org.datakow.catalogs.metadata.jsonschema.JsonSchema;
import org.datakow.core.components.DotNotationList;
import org.datakow.core.components.DotNotationMap;
import org.datakow.messaging.events.CatalogEventsSenderClient;
import org.datakow.messaging.events.events.CatalogEvent;
import org.datakow.messaging.events.events.EventAction;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.datakow.catalogs.metadata.Catalog;
import org.datakow.catalogs.metadata.CatalogRegistry;
import org.datakow.catalogs.metadata.DataRetentionPolicy;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestController;

/**
 *
 * @author kevin.off
 */
@CrossOrigin
@RestController
@RequestMapping(value = {"/"})
public class ManagementRequestController {
    
    @Autowired
    MongoDBMetadataCatalogManagementDao managementDao;

    @Autowired
    CatalogEventsSenderClient eventsSenderClient;

    @Autowired
    CatalogRegistry catalogRegistry;
    
    private final Logger logger;
    
    public ManagementRequestController() {
        logger = LogManager.getLogger(this.getClass());
    }
    
    @RequestMapping(value = "/catalogs", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<DotNotationMap> createCatalog(
            HttpServletRequest request, 
            HttpServletResponse response, 
            @RequestParam(value = "Catalog-Identifier") String catalogIdentifier,
            @RequestParam(value = "indexStorageObject", required = false, defaultValue = "true") boolean indexStorageObject,
            @RequestHeader Map<String, String> headers) throws IOException {
        
        logger.info("Received a create catalog request for catalog: " + catalogIdentifier);
        
        if (!hasAccess(headers.get("authorization"))){
            return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        }
        
        DotNotationMap returnMap = new DotNotationMap();
        returnMap.setProperty("Request", "Create Catalog");
        returnMap.setProperty("Catalog-Identifier", catalogIdentifier);
        
        boolean createCatalogResponse = managementDao.createCatalog(catalogIdentifier, catalogIdentifier, indexStorageObject, "datakow");
        returnMap.setProperty("Success", createCatalogResponse);
        if (createCatalogResponse){
            CatalogEvent event = new CatalogEvent(EventAction.CREATED);
            event.setEventId(UUID.randomUUID().toString());
            event.setCatalogIdentifier(catalogIdentifier);
            eventsSenderClient.sendEvent(event, catalogIdentifier, "");
            return new ResponseEntity<>(returnMap, HttpStatus.CREATED);
        }else{
            return new ResponseEntity<>(returnMap, HttpStatus.INTERNAL_SERVER_ERROR);
        }
            
       
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<DotNotationMap> deleteCatalog(
            HttpServletRequest request, 
            HttpServletResponse response, 
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @RequestHeader Map<String, String> headers) throws IOException {
        
        if (!hasAccess(headers.get("authorization"))){
            return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        }

        boolean deleted = managementDao.deleteCatalog(catalogIdentifier);
        
        DotNotationMap map = new DotNotationMap();
        map.setProperty("Success", deleted);
        map.setProperty("Request", "Delete Catalog");
        map.setProperty("Catalog-Identifier", catalogIdentifier);
        
        if (deleted){
            CatalogEvent event = new CatalogEvent(EventAction.DELETED);
            event.setEventId(UUID.randomUUID().toString());
            event.setCatalogIdentifier(catalogIdentifier);
            eventsSenderClient.sendEvent(event, catalogIdentifier, "");
            return new ResponseEntity<>(map, HttpStatus.OK);
        }else{
            return new ResponseEntity<>(map, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/schema", method = {RequestMethod.POST, RequestMethod.PUT}, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<DotNotationMap> createSchema(
            HttpServletRequest request, 
            HttpServletResponse response, 
            @PathVariable("catalogIdentifier") String catalogIdentifier,
            @RequestHeader Map<String, String> headers) throws IOException {
        
        if (!hasAccess(headers.get("authorization"))){
            return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        }
        
        String body = IOUtils.toString(request.getInputStream());
        LogManager.getLogger(this.getClass()).debug("About to ingest a schema for the " + catalogIdentifier + " catalog\n" + body + "\n");
        DotNotationMap requestMap = DotNotationMap.fromJson(body);
        
        DotNotationMap map = new DotNotationMap();
        boolean result = false;
        map.setProperty("Request", "Save Schema");
        map.setProperty("Catalog-Identifier", catalogIdentifier);
        JsonSchema schema = null;
        if (requestMap != null && requestMap.containsKey("$schema")){
            schema = JsonSchema.fromJson(requestMap.toJson());
            result = managementDao.saveSchema(catalogIdentifier, schema, "datakow");
            map.setProperty("Success", result);
        }
        
        if (result){
            CatalogEvent event = new CatalogEvent(EventAction.UPDATED);
            event.setEventId(UUID.randomUUID().toString());
            event.setCatalogIdentifier(catalogIdentifier);
            eventsSenderClient.sendEvent(event, catalogIdentifier, "Schema");
            return new ResponseEntity<>(map, HttpStatus.CREATED);
        }else{
            return new ResponseEntity<>(map, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/schema", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<JsonSchema> getSchema(
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            HttpServletRequest request, 
            HttpServletResponse response) throws IOException {
        
        JsonSchema schema = managementDao.getSchema(catalogIdentifier);
        
        if (schema != null){
            return new ResponseEntity<>(schema, HttpStatus.OK);
        }else{
            return new ResponseEntity<>(new JsonSchema(), HttpStatus.NOT_FOUND);
        }
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/schema", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<DotNotationMap> deleteSchema(
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            HttpServletRequest request, 
            HttpServletResponse response,
            @RequestHeader Map<String, String> headers) throws IOException {
        
        if (!hasAccess(headers.get("authorization"))){
            return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        }
        
        DotNotationMap map = new DotNotationMap();
        boolean result = managementDao.deleteSchema(catalogIdentifier);
        map.setProperty("Request", "Delete Schema");
        map.setProperty("Catalog-Identifier", catalogIdentifier);
        map.setProperty("Success", result);
        
        if (result){
            CatalogEvent event = new CatalogEvent(EventAction.UPDATED);
            event.setEventId(UUID.randomUUID().toString());
            event.setCatalogIdentifier(catalogIdentifier);
            eventsSenderClient.sendEvent(event, catalogIdentifier, "Schema");
            return new ResponseEntity<>(map, HttpStatus.OK);
        }else{
            return new ResponseEntity<>(map, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        
    }
    
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/retention", method = {RequestMethod.POST, RequestMethod.PUT}, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<DotNotationMap> saveDataRetentionPolicy(
            HttpServletRequest request, 
            HttpServletResponse response, 
            @PathVariable("catalogIdentifier") String catalogIdentifier,
            @RequestHeader Map<String, String> headers) throws IOException {
        
        if (!hasAccess(headers.get("authorization"))){
            return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        }
        
        DotNotationList requestList = DotNotationList.fromJson(IOUtils.toString(request.getInputStream()));
        
        DotNotationMap responseMap = new DotNotationMap();
        boolean result = false;
        responseMap.setProperty("Request", "Save Data Retention Policy");
        responseMap.setProperty("Catalog-Identifier", catalogIdentifier);
        List<DataRetentionPolicy> policies = null;

        if (requestList != null){
            policies = new ArrayList<>();
            for(Object i : requestList){
                if (i instanceof DotNotationMap){
                    policies.add(DataRetentionPolicy.fromJson(((DotNotationMap)i).toJson()));
                }
            }
            result = managementDao.saveDataRetentionPolicy(catalogIdentifier, policies, "datakow");
            responseMap.setProperty("Success", result);
        }
        
        if (result){
            CatalogEvent event = new CatalogEvent(EventAction.UPDATED);
            event.setEventId(UUID.randomUUID().toString());
            event.setCatalogIdentifier(catalogIdentifier);
            eventsSenderClient.sendEvent(event, catalogIdentifier, "Retention-Policy");
            return new ResponseEntity<>(responseMap, HttpStatus.CREATED);
        }else{
            return new ResponseEntity<>(responseMap, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/retention", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<List<DataRetentionPolicy>> getDataRetentionPolicy(
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            HttpServletRequest request, 
            HttpServletResponse response) throws IOException {
        
        List<DataRetentionPolicy> policies = managementDao.getDataRetentionPolicy(catalogIdentifier);
        
        if (policies != null){
            return new ResponseEntity<>(policies, HttpStatus.OK);
        }else{
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/retention", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<DotNotationMap> deleteDataRetentionPolicy(
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            HttpServletRequest request, 
            HttpServletResponse response,
            @RequestHeader Map<String, String> headers) throws IOException {
        
        if (!hasAccess(headers.get("authorization"))){
            return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        }
        
        DotNotationMap map = new DotNotationMap();
        boolean result = managementDao.deleteDataRetentionPolicy(catalogIdentifier, "datakow");
        map.setProperty("Request", "Delete Data Retention Policy");
        map.setProperty("Catalog-Identifier", catalogIdentifier);
        map.setProperty("Success", result);
        
        if (result){
            CatalogEvent event = new CatalogEvent(EventAction.UPDATED);
            event.setEventId(UUID.randomUUID().toString());
            event.setCatalogIdentifier(catalogIdentifier);
            eventsSenderClient.sendEvent(event, catalogIdentifier, "Retention-Policy");
            return new ResponseEntity<>(map, HttpStatus.OK);
        }else{
            return new ResponseEntity<>(map, HttpStatus.INTERNAL_SERVER_ERROR);
        }
        
    }
    
    
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/indexes", method = RequestMethod.POST, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<DotNotationMap> createIndexes(
            HttpServletRequest request, 
            HttpServletResponse response, 
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @RequestHeader Map<String, String> headers) throws IOException {
      
        if (!hasAccess(headers.get("authorization"))){
            return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        }
        
        String indexDefinitionJson = IOUtils.toString(request.getInputStream());
        List<MongoIndex> indexes = MongoIndex.fromJson(indexDefinitionJson);
        
        boolean created = true;
        for(MongoIndex index : indexes){
            created = created && managementDao.createIndex(catalogIdentifier, index);
        }
        
        DotNotationMap returnMap = new DotNotationMap();
        returnMap.setProperty("Catalog-Identifier", catalogIdentifier);
        returnMap.setProperty("Request", "Create Index");
        returnMap.setProperty("Index-Names", indexes.stream().map(i->i.getName()).collect(Collectors.joining(", ")));
        
        returnMap.setProperty("Success", created);
        
        if (created){
            CatalogEvent event = new CatalogEvent(EventAction.UPDATED);
            event.setEventId(UUID.randomUUID().toString());
            event.setCatalogIdentifier(catalogIdentifier);
            eventsSenderClient.sendEvent(event, catalogIdentifier, "Indexes");
            return new ResponseEntity<>(returnMap, HttpStatus.CREATED);
        }else{
            return new ResponseEntity<>(returnMap, HttpStatus.INTERNAL_SERVER_ERROR);
        } 
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/indexes/{indexName:.+}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<String> getIndex(
            HttpServletRequest request, 
            HttpServletResponse response, 
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @PathVariable("indexName") String indexName) throws IOException{
        
        List<MongoIndex> indexes = managementDao.getIndexes(catalogIdentifier);
       
        Optional<MongoIndex> i = indexes.stream().filter(f -> f.getName().equals(indexName)).findFirst();
        if (i.isPresent()){
            return new ResponseEntity<>(i.get().toJson(), HttpStatus.OK);
        }else{
            return new ResponseEntity<>("{\"Reason:\":\"Cannot find index named " + indexName + " in catalog " + catalogIdentifier + "\"}", HttpStatus.NOT_FOUND);
        }
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/indexes/{indexName:.+}", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<DotNotationMap> deleteIndex(
            HttpServletRequest request, 
            HttpServletResponse response, 
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @PathVariable("indexName") String indexName, 
            @RequestHeader Map<String, String> headers) throws IOException {
        
        if (!hasAccess(headers.get("authorization"))){
            return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        }
        
        boolean deleted = managementDao.deleteIndex(catalogIdentifier, indexName);
        DotNotationMap map = new DotNotationMap();
        map.setProperty("Success", deleted);
        map.setProperty("Catalog-Identifier", catalogIdentifier);
        map.setProperty("Index-Name", indexName);
        map.setProperty("Request", "Delete Index");
        if (deleted){
            CatalogEvent event = new CatalogEvent(EventAction.UPDATED);
            event.setEventId(UUID.randomUUID().toString());
            event.setCatalogIdentifier(catalogIdentifier);
            eventsSenderClient.sendEvent(event, catalogIdentifier, "Indexes");
            return new ResponseEntity<>(map, HttpStatus.OK);
        }else{
            return new ResponseEntity<>(map, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/indexes", method = RequestMethod.DELETE, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<DotNotationMap> deleteIndexes(
            HttpServletRequest request, 
            HttpServletResponse response, 
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @RequestHeader Map<String, String> headers) throws IOException {
        
        if (!hasAccess(headers.get("authorization"))){
            return new ResponseEntity<>(HttpStatus.UNAUTHORIZED);
        }
        
        boolean deleted = managementDao.deleteAllIndexes(catalogIdentifier);
        DotNotationMap map = new DotNotationMap();
        map.setProperty("Success", deleted);
        map.setProperty("Catalog-Identifier", catalogIdentifier);
        map.setProperty("Request", "Delete all indexes");
        if (deleted){
            CatalogEvent event = new CatalogEvent(EventAction.UPDATED);
            event.setEventId(UUID.randomUUID().toString());
            event.setCatalogIdentifier(catalogIdentifier);
            eventsSenderClient.sendEvent(event, catalogIdentifier, "Indexes");
            return new ResponseEntity<>(map, HttpStatus.OK);
        }else{
            return new ResponseEntity<>(map, HttpStatus.INTERNAL_SERVER_ERROR);
        }
    }
    
    @RequestMapping(value = "/catalogs", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    @ResponseStatus(HttpStatus.OK)
    public HttpEntity<List<Catalog>> getCatalogs(
            HttpServletRequest request, 
            @RequestParam(value = "includeIndexes", required = false, defaultValue = "false") boolean includeIndexes,
            @RequestParam(value = "includeStats", required = false, defaultValue = "false") boolean includeStats,
            HttpServletResponse response) throws IOException {
        
        List<Catalog> catalogs = managementDao.getAllCatalogs(includeIndexes, includeStats);
        
        return new ResponseEntity<>(catalogs, HttpStatus.OK);
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<Catalog> getCatalog(
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @RequestParam(value = "includeIndexes", required = false, defaultValue = "false") boolean includeIndexes,
            @RequestParam(value = "includeStats", required = false, defaultValue = "false") boolean includeStats,
            HttpServletRequest request, 
            HttpServletResponse response) throws IOException {
        
        Catalog catalog = managementDao.getCatalogByCatalogIdentifier(catalogIdentifier, includeIndexes, includeStats);
        
        if (catalog != null){
            return new ResponseEntity<>(catalog, HttpStatus.OK);
        }else{
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/indexes", method = RequestMethod.GET, produces = MediaType.APPLICATION_JSON_VALUE)
    public HttpEntity<List<MongoIndex>> getIndexes(
            @PathVariable("catalogIdentifier") String catalogIdentifier) throws IOException {
        
        List<MongoIndex> map = managementDao.getIndexes(catalogIdentifier);
        if (map.size() > 0){
            return new ResponseEntity<>(map, HttpStatus.OK);
        }else{
            return new ResponseEntity<>(map, HttpStatus.NOT_FOUND);
        }
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}", method = RequestMethod.PUT)
    public HttpEntity setProperty(
            @PathVariable("catalogIdentifier") String catalogIdentifier, 
            @RequestParam Map<String, String> params,
            HttpServletRequest request) throws IOException{
        
        Catalog catalog = managementDao.getCatalogByCatalogIdentifier(catalogIdentifier, false, false);
        
        if (catalog != null){
        
            DotNotationMap catalogMap = DotNotationMap.fromJson(catalog.toJson());
            for(Map.Entry<String, String> entry : params.entrySet()){
                catalogMap.setProperty(entry.getKey(), entry.getValue());
            }
            catalog = Catalog.fromJson(catalogMap.toJson());
            managementDao.updateCatalog(catalog, "datakow");
            CatalogEvent event = new CatalogEvent(EventAction.UPDATED);
            event.setEventId(UUID.randomUUID().toString());
            event.setCatalogIdentifier(catalogIdentifier);
            eventsSenderClient.sendEvent(event, catalogIdentifier, params.keySet().stream().collect(Collectors.joining(",")));
            
            return new ResponseEntity(HttpStatus.OK);
        }else{
            return new ResponseEntity("Catalog " + catalogIdentifier + " does not exist.", HttpStatus.BAD_REQUEST);
        }
        
    }
    
    @RequestMapping(value = "/catalogs/{catalogIdentifier}/{propertyName}", method = RequestMethod.GET, produces = MediaType.TEXT_PLAIN_VALUE)
    public HttpEntity<String> getCatalogProperty(
            @PathVariable("catalogIdentifier") String catalogIdentifier,
            @PathVariable("propertyName") String propertyName) throws IOException{
        
        Catalog catalog = managementDao.getCatalogByCatalogIdentifier(catalogIdentifier, true, true);
        
        if (catalog != null){
            
            DotNotationMap catalogMap = DotNotationMap.fromJson(catalog.toJson());
            String value = catalogMap.getProperty(propertyName);
            
            return new ResponseEntity<>(value, HttpStatus.OK);
        }else{
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
    
    private boolean hasAccess(String authorization){
        
        String correctAuth = "Basic ZGF0YWtvdzpkYXRha293";
        
        return authorization != null && authorization.equals(correctAuth);
        
    }
    
}
