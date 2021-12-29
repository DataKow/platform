package org.datakow.apps.notificationrouter;

import org.datakow.messaging.events.CatalogEventsReceiverClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.actuate.health.Health;
import org.springframework.boot.actuate.health.HealthIndicator;
import org.springframework.stereotype.Component;

/**
 *
 * @author kevin.off
 */
@Component
public class QueuesHealthIndicator implements HealthIndicator {
    
    @Autowired
    private CatalogEventsReceiverClient catalogEventsReceiverClient;

    @Override
    public Health health() {
        boolean isRunning = catalogEventsReceiverClient.getClientListenerContainer().isRunning();
        int queues = catalogEventsReceiverClient.getClientListenerContainer().getQueueNames().length;
        Health.Builder b;
        if (isRunning && queues == 2){
            b = Health.up();
        }else{
            b = Health.down();
        }
        return b
                .withDetail("Is-Running", isRunning)
                .withDetail("Num-Queues-Required", 2)
                .withDetail("Num-Queues-Listening", queues)
                .build();
    } 
}
