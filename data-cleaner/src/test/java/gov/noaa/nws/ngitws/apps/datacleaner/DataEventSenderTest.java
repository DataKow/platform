/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package gov.noaa.nws.ngitws.apps.datacleaner;

import gov.noaa.nws.ngitws.catalogs.metadata.DataRetentionPolicy;
import java.util.Date;
import org.junit.Assert;
import org.junit.Test;

/**
 *
 * @author kevin.off
 */
public class DataEventSenderTest {
    
    public DataEventSenderTest() {
    }

    @Test
    public void testGetRetentionDate() {
        DataRetentionPolicy policy = new DataRetentionPolicy();
        policy.setRetentionPeriodInDays(1);
        DataEventSender sender = new DataEventSender();
        Date retention = sender.getRetentionDate(policy);
        Date now = new Date();
        String s = "bob";
    }
    
}
