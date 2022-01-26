/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.noaa.nws.ngitws.apps.datacleaner;

import gov.noaa.nws.ngitws.catalogs.metadata.webservice.MetadataCatalogWebserviceClient;
import gov.noaa.nws.ngitws.catalogs.object.webservice.ObjectCatalogWebserviceClient;
import gov.noaa.nws.ngitws.messaging.events.CatalogEventsReceiver;
import gov.noaa.nws.ngitws.messaging.events.events.DataCleanerEvent;
import gov.noaa.nws.ngitws.messaging.events.events.Event;
import org.apache.logging.log4j.LogManager;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

/**
 *
 * @author kevin.off
 */
@Service
public class EventReceiver implements CatalogEventsReceiver{

    @Autowired
    MetadataCatalogWebserviceClient metaClient;
    
    @Autowired
    ObjectCatalogWebserviceClient objClient;
    
    @Override
    public void receiveEvent(Event event) {

        if (event instanceof DataCleanerEvent){
            DataCleanerEvent dcEvent = (DataCleanerEvent)event;
            
            String catalogIdentifier = dcEvent.getCatalogIdentifier();
            int numDeleted;
            if(catalogIdentifier.equals("NGITWS_OBJECTS")){
                numDeleted = objClient.deleteByQuery(dcEvent.getCatalogIdentifier(), dcEvent.getFiql(), -1);
            }else{
                numDeleted = metaClient.deleteByQuery(dcEvent.getCatalogIdentifier(), dcEvent.getFiql());
            }
            
            LogManager.getLogger().debug("Deleted " + numDeleted + " record from " + catalogIdentifier);
        }
    }
    
}
