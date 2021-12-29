package org.datakow.apps.subscriptionwebservice;

import org.datakow.security.access.Read;
import org.datakow.security.access.Realm;
import org.datakow.security.access.RealmAccessProvider;
import org.datakow.security.access.Write;
import java.util.ArrayList;
import java.util.List;
import org.springframework.stereotype.Component;

/**
 *
 * @author kevin.off
 */
@Component
public class SubscriptionWebServiceRoleAccessProvider implements RealmAccessProvider{

    @Override
    public List<Realm> getRealmAccess() {
        List<Realm> realms = new ArrayList<>();
        Realm realm = new Realm("*", new Read("ROLE_ADMIN", "ROLE_ANONYMOUS", "ROLE_INTERNAL"), new Write("ROLE_ADMIN", "ROLE_ANONYMOUS", "ROLE_INTERNAL"));
        realms.add(realm);
        return realms;
    }
    
}
