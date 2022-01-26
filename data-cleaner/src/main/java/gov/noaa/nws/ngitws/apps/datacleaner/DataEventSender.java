/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.noaa.nws.ngitws.apps.datacleaner;

import com.fasterxml.jackson.core.JsonProcessingException;
import gov.noaa.nws.ngitws.catalogs.metadata.Catalog;
import gov.noaa.nws.ngitws.catalogs.metadata.CatalogRegistry;
import gov.noaa.nws.ngitws.catalogs.metadata.DataRetentionPolicy;
import gov.noaa.nws.ngitws.catalogs.metadata.MetadataCatalogRecord;
import gov.noaa.nws.ngitws.catalogs.metadata.database.MetadataDataCoherence;
import gov.noaa.nws.ngitws.catalogs.metadata.webservice.MetadataCatalogManagementWebserviceClient;
import gov.noaa.nws.ngitws.catalogs.metadata.webservice.MetadataCatalogWebserviceClient;
import gov.noaa.nws.ngitws.catalogs.metadata.webservice.MetadataCatalogWebserviceRequest;
import gov.noaa.nws.ngitws.catalogs.object.database.ObjectDataCoherence;
import gov.noaa.nws.ngitws.catalogs.object.webservice.ObjectCatalogWebserviceClient;
import gov.noaa.nws.ngitws.configuration.rabbit.ExclusiveLock;
import gov.noaa.nws.ngitws.configuration.rabbit.RabbitClient;
import gov.noaa.nws.ngitws.configuration.rabbit.configuration.RabbitConfigurationProperties;
import gov.noaa.nws.ngitws.core.components.CatalogIdentity;
import gov.noaa.nws.ngitws.core.components.DotNotationMap;
import gov.noaa.nws.ngitws.messaging.events.CatalogEventsSenderClient;
import gov.noaa.nws.ngitws.messaging.events.events.DataCleanerEvent;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

/**
 *
 * @author kevin.off
 */
@Service
public class DataEventSender {
    
    @Autowired
    MetadataCatalogManagementWebserviceClient managementDao;
    
    @Autowired
    MetadataCatalogWebserviceClient metaClient;
    
    @Autowired
    ObjectCatalogWebserviceClient objClient;
    
    @Autowired
    RabbitClient rabbitClient;
    
    @Autowired
    ExclusiveLock lock;
    
    @Autowired
    RabbitConfigurationProperties rabbitProps;
    
    @Autowired
    CatalogEventsSenderClient eventSender;
    
    @Autowired
    CatalogRegistry catalogsRegistry;
    
    @Autowired
    EventReceiver receiver;
    
    @Value("${ngitws.datacleaner.queryBatchSize}")
    int queryBatchSize;
    @Value("${ngitws.datacleaner.maxNumQueryBatchesPerPolicy}")
    int maxNumQueryBatchesPerPolicy;
    @Value("${ngitws.datacleaner.deleteBatchSize}")
    int deleteBatchSize;
    @Value("${ngitws.datacleaner.maxQueueSize}")
    int queueThreshold;
    @Value("${ngitws.datacleaner.maxQueueSize}")
    int maxQueueSize;
    @Value("${ngitws.datacleaner.minQueueSize}")
    int minQueueSize;
    
    Integer queueSize;
    
    private void manageQueue(){

        if (queueSize > maxQueueSize && queueThreshold != minQueueSize){
            LogManager.getLogger().info("Max reached. Setting to min.");
            queueThreshold = minQueueSize;
        }else if (queueSize <= minQueueSize && queueThreshold != maxQueueSize){
            LogManager.getLogger().info("Min reached. Setting to max.");
            queueThreshold = maxQueueSize;
        }
        
    }
    
    @Scheduled(fixedDelayString = "${ngitws.datacleaner.fixedDelayMs}")
    public void sliceAndSendEvents() throws JsonProcessingException, IOException{
        
        String queueName = "q.datacleaner.catalog.events.DataCleanerService." + rabbitProps.getMessagingVersion();
        queueSize = rabbitClient.getQueueSize(queueName);
        
        if (!lock.isLockAcquired()){
            return;
        }
       
        if (queueSize == null){
            throw new IllegalStateException("The data cleaner cannot detect its service queue " + queueName);
        }
        
        manageQueue();
        
        if(queueSize > queueThreshold){
            return;
        }
        
        Collection<Catalog> catalogs = catalogsRegistry.getAllCatalogsFromCache();
        
        //For each catalog
        for (Catalog catalog : catalogs) {
            String catalogIdentifier = catalog.getCatalogIdentifier();
            if (catalogIdentifier.equals("NGITWS_OBJECTS")){
                continue;
            }
            //If there is a policy
            if (catalog.getDataRetentionPolicy() != null) {
                //For each policy
                for (DataRetentionPolicy policy : catalog.getDataRetentionPolicy()) {
                    
                    int batchNumber = 1;
                    boolean more = true;
                    //If there are more records and we have not exceeded the number of batches per policy
                    while(more && batchNumber <= maxNumQueryBatchesPerPolicy){
                    
                        batchNumber++;
                        //Make the query to find all old metadata catalog records
                        Date retentionDate = getRetentionDate(policy);
                        String searchFiql = policy.getRetentionDateKey() + "=lt=" + getQueryFormattedDate(retentionDate);
                        if (!StringUtils.isEmpty(policy.getRetentionFilter())){
                            searchFiql = searchFiql + ";" + policy.getRetentionFilter();
                        }
                        if (minQueueSize > 0){
                            searchFiql = searchFiql + ";Doc.markForDelete==null";
                        }
                        Long start = System.currentTimeMillis();
                        List<MetadataCatalogRecord> oldRecordsResult = metaClient.getByQueryAndStream(
                                catalogIdentifier, 
                                MetadataCatalogWebserviceRequest.builder()
                                        .withQuery(searchFiql)
                                        .withSort(policy.getRetentionDateKey() + " ASC")
                                        .withLimit(queryBatchSize) 
                                        .withProjectionProperties(Arrays.asList("Storage.Object-Identities", "Storage.Record-Identifier")) 
                                        .withDataCoherence(MetadataDataCoherence.AVAILABLE)).toList();
                        Long end = System.currentTimeMillis();
                        if (oldRecordsResult.isEmpty()){
                            break;
                        }
                        
                        long diff = end - start;
                        LogManager.getLogger().info("Query for " + queryBatchSize + " " + catalogIdentifier + " records took " + diff + " ms.");
                        
                        //If we found exactly the number we asked for then there are most likely more
                        more = oldRecordsResult.size() == queryBatchSize;

                        List<List<MetadataCatalogRecord>> oldRecordList = new ArrayList<>();
                        
                        List<MetadataCatalogRecord> records = new ArrayList<>();
                        for(MetadataCatalogRecord record : oldRecordsResult){
                            //add the record to the delete batch
                            records.add(record);
                            //if the number of records in the batch equals the delete batch size
                            if (records.size() == deleteBatchSize){
                                //add the batch to the batches
                                oldRecordList.add(records);
                                //make a new batch
                                records = new ArrayList<>();
                            }
                        }
                        //If there are some records that didn't make it into a batch
                        //because there were fewer than deleteBatchSize then make a batch out of them
                        if (!records.isEmpty()){
                            oldRecordList.add(records);
                        }

                        //For each batch of deletes
                        for(List<MetadataCatalogRecord> recordsToProcess : oldRecordList){

                            //Make a new delete key for each batch
                            String metadataDeleteUUID = UUID.randomUUID().toString();
                            String objectDeleteUUID = UUID.randomUUID().toString();
                            //Make the merge patch for the update
                            DotNotationMap objectMergePatch = new DotNotationMap();
                            objectMergePatch.setProperty("metadata.markForDelete", objectDeleteUUID);
                            DotNotationMap metadataMergePatch = new DotNotationMap();
                            metadataMergePatch.setProperty("Doc.markForDelete", metadataDeleteUUID);

                            Map<String, List<String>> objectsToMark = new HashMap<>();
                            List<String> metadataToMark = new ArrayList<>();

                            //for each Metadata Record that needs deleted
                            for(MetadataCatalogRecord oldRecord : recordsToProcess){
                                //If there are any objects associated
                                if (oldRecord.getStorage().getObjectIdentities() != null){
                                    //For each identity that was associated
                                    for(CatalogIdentity oldObjectIdentity : oldRecord.getStorage().getObjectIdentities()){
                                        //If the map doesn't contain the object catalog key then add a new list for the recordIds
                                        if (!objectsToMark.containsKey(oldObjectIdentity.getCatalogIdentifier())){
                                            objectsToMark.put(oldObjectIdentity.getCatalogIdentifier(), new ArrayList<>());
                                        }
                                        //Add the object's record identifier to the list
                                        objectsToMark.get(oldObjectIdentity.getCatalogIdentifier()).add(oldObjectIdentity.getRecordIdentifier());

                                        //If the list is getting much larger than the max deleteBatchSize then we need to go
                                        //ahead and send the request. For objects, we have to send the request to delete them
                                        if (objectsToMark.get(oldObjectIdentity.getCatalogIdentifier()).size() > deleteBatchSize + 50){
                                            
                                            objClient.mergePatchByQuery(
                                                    oldObjectIdentity.getCatalogIdentifier(),
                                                    "Record-Identifier=in=(" + String.join(",", objectsToMark.get(oldObjectIdentity.getCatalogIdentifier())) + ")", 
                                                    objectMergePatch, 
                                                    true);
                                            objectsToMark.get(oldObjectIdentity.getCatalogIdentifier()).clear();

                                            //Send the event for this batch
                                            sendEvent(oldObjectIdentity.getCatalogIdentifier(), "/metadata.markForDelete==" + objectDeleteUUID);

                                            //Make a new batch ID
                                            objectDeleteUUID = UUID.randomUUID().toString();
                                            objectMergePatch.setProperty("metadata.markForDelete", objectDeleteUUID);
                                        }
                                    }
                                }

                                //After we have all of the associated objects taken care of, add the metadata id to the list
                                metadataToMark.add(oldRecord.getStorage().getId());


                            }//End foreach old record

                            //For each object that needs to be deleted send the merge patch
                            for(String objectCatalogIdentifier : objectsToMark.keySet()){

                                sendObjectMergePatchAndEvent(objectCatalogIdentifier, objectsToMark.get(objectCatalogIdentifier), objectMergePatch, objectDeleteUUID);

                                //Make a new batch ID
                                objectDeleteUUID = UUID.randomUUID().toString();
                                objectMergePatch.setProperty("metadata.markForDelete", objectDeleteUUID);
                            }

                            //If there are any metadata records to mark
                            if (!metadataToMark.isEmpty()){
                                //Send the merge patch for the metadata records
                                sendMetadataMergePatchAndEvent(catalogIdentifier, metadataToMark, metadataMergePatch, metadataDeleteUUID);
                            }

                        }//End foreach delete batch
                    
                    }//End while(more && batchNum <= maxNumQueryBatchesPerPolicy)
                    
                }//End foreach retention policy
            }
        }//End of for each metadata catalog
        Catalog objectCatalog = catalogsRegistry.getByCatalogIdentifier("NGITWS_OBJECTS");
        if (objectCatalog == null){
            return;
        }
        if (objectCatalog.getDataRetentionPolicy() != null){
            for(DataRetentionPolicy policy : objectCatalog.getDataRetentionPolicy()){
                //Make the query to find all old metadata catalog records
                Date retentionDate = getObjectSafeRetentionDate(policy);
                String searchFiql = policy.getRetentionDateKey() + "=lt=" + getQueryFormattedDate(retentionDate);
                if (!StringUtils.isEmpty(policy.getRetentionFilter())){
                    searchFiql = searchFiql + ";" + policy.getRetentionFilter();
                }
                if (minQueueSize > 0){
                    searchFiql = searchFiql + ";/metadata.markForDelete==null";
                }
                Long start = System.currentTimeMillis();
                List<String> ids = objClient.getByQuery(
                        objectCatalog.getCatalogIdentifier(), 
                        searchFiql, 
                        "Publish-Date ASC", 
                        queryBatchSize, "legacy", 
                        ObjectDataCoherence.AVAILABLE);
                Long end = System.currentTimeMillis();
                long diff = end - start;
                LogManager.getLogger().info("Query for " + queryBatchSize + " " + objectCatalog.getCatalogIdentifier() + " records took " + diff + " ms.");
                //Make a new delete key for each batch
                String objectDeleteUUID = UUID.randomUUID().toString();
                //Make the merge patch for the update
                DotNotationMap objectMergePatch = new DotNotationMap();
                objectMergePatch.setProperty("metadata.markForDelete", objectDeleteUUID);
                
                if(!ids.isEmpty()){
                    sendObjectMergePatchAndEvent(objectCatalog.getCatalogIdentifier(), ids, objectMergePatch, objectDeleteUUID);
                }
            }
        }
    }
    
    public void sendMetadataMergePatchAndEvent(String catalogIdentifier, List<String> metadataToMark, DotNotationMap metadataMergePatch, String metadataDeleteUUID) throws JsonProcessingException{
        
        metaClient.mergePatchByQuery(catalogIdentifier, 
                    "Storage.Record-Identifier=in=(" + String.join(",", metadataToMark) + ")", 
                    null,
                    metadataMergePatch, 
                    true, false);

        //Send the event for this batch
        sendEvent(catalogIdentifier, "Doc.markForDelete==" + metadataDeleteUUID);
        
    }
    
    public void sendObjectMergePatchAndEvent(String catalogIdentifier, List<String> objectsToMark, DotNotationMap objectMergePatch, String objectDeleteUUID) throws JsonProcessingException{
        
        objClient.mergePatchByQuery(
                catalogIdentifier, 
                "Record-Identifier=in=(" + String.join(",", objectsToMark) + ")", 
                objectMergePatch, 
                true);

        //Send the event for this batch
        sendEvent(catalogIdentifier, "/metadata.markForDelete==" + objectDeleteUUID);
    }
    
    public void sendEvent(String catalogIdentifier, String fiql){
        
        DataCleanerEvent event =  new DataCleanerEvent(catalogIdentifier, fiql);
        eventSender.sendEvent(event, "DataCleanerService", "clean", "records");
    }
    
    Date getRetentionDate(DataRetentionPolicy settings) {
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.add(Calendar.DAY_OF_MONTH, settings.getRetentionPeriodInDays() * -1);
        return calendar.getTime();
    }

    private Date getObjectSafeRetentionDate(DataRetentionPolicy settings) {
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        Calendar calendar = Calendar.getInstance(timeZone);
        calendar.add(Calendar.DAY_OF_MONTH, settings.getRetentionPeriodInDays() * -1);
        calendar.add(Calendar.HOUR_OF_DAY, -6);
        return calendar.getTime();
    }
    
    private String getQueryFormattedDate(Date date) {
        TimeZone timeZone = TimeZone.getTimeZone("UTC");
        SimpleDateFormat queryFormat = new SimpleDateFormat("yyyyMMdd'T'HHmmss'Z'");
        queryFormat.setTimeZone(timeZone);
        return queryFormat.format(date);
    }
    
}
