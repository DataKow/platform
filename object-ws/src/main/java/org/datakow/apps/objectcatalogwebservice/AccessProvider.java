package org.datakow.apps.objectcatalogwebservice;


import org.datakow.security.access.Read;
import org.datakow.security.access.Realm;
import java.util.Arrays;
import java.util.List;
import org.springframework.stereotype.Component;
import org.datakow.security.access.RealmAccessProvider;
import org.datakow.security.access.Write;

/**
 *
 * @author kevin.off
 */
@Component
public class AccessProvider implements RealmAccessProvider {
 
    @Override
    public List<Realm> getRealmAccess() {
        Realm allAccessPass = new Realm("*", new Read("*"), new Write("*"));
        return Arrays.asList(allAccessPass);
    }
    
}
