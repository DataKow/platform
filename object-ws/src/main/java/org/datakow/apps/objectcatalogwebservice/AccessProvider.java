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
//        Realm amdar = new Realm("amdar", new Read("ROLE_AMDAR"), new Write());
//        Realm nullRealm = new Realm(null, new Read("*"), new Write("*"));
//        Realm emptyRealm = new Realm("", new Read("*"), new Write("*"));
//        Realm publicRealm = new Realm("public", new Read("*"), new Write("*"));
//        Realm upperPublicRealm = new Realm("Public", new Read("*"), new Write("*"));
//        Realm allRealms = new Realm("*", new Read("ROLE_ADMIN"), new Write("ROLE_ADMIN"));
//        return Arrays.asList(allRealms, nullRealm, emptyRealm, publicRealm, upperPublicRealm, amdar);
        return Arrays.asList(allAccessPass);
    }
    
}
