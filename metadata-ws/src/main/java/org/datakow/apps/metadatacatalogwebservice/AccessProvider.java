package org.datakow.apps.metadatacatalogwebservice;

import org.datakow.security.access.Realm;
import org.datakow.security.access.RealmAccessProvider;
import java.util.Arrays;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 *
 * @author kevin.off
 */
@Component
public class AccessProvider implements RealmAccessProvider {
 
    @Override
    public List<Realm> getRealmAccess() {
        Realm amdar = new Realm("amdar");
        amdar.addReadingRoles("ROLE_AMDAR");
        return Arrays.asList(amdar);
    }
    
}
