/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.noaa.nws.ngitws.apps.datacleaner;

import gov.noaa.nws.ngitws.messaging.events.CatalogEventsReceiverClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.stereotype.Component;

/**
 *
 * @author kevin.off
 */
@Component
public class Init implements CommandLineRunner{

    @Autowired
    CatalogEventsReceiverClient client;
    
    @Override
    public void run(String... args) throws Exception {
        client.startReceivingEventsSharedService("DataCleanerService", "datacleaner");
    }
    
}
