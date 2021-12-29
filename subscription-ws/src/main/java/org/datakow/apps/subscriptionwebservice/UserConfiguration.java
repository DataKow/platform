package org.datakow.apps.subscriptionwebservice;

import org.datakow.security.HardCodedUserConfiguration;
import java.util.ArrayList;
import java.util.Collection;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.userdetails.User;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.stereotype.Component;

/**
 *
 * @author kevin.off
 */
@Component
public class UserConfiguration implements HardCodedUserConfiguration{
    
    @Override
    public Collection<UserDetails> getUsers() {
        ArrayList<UserDetails> users = new ArrayList<>();
        
        users.add(createUser("datakow", "{noop}datakow", "ROLE_ADMIN"));
        
        return users;
    }
    
    private User createUser(String username, String password, String ... authorityStrings){
        ArrayList<GrantedAuthority> grantedAuthority = new ArrayList<>();
        for(String authorityString : authorityStrings){
            grantedAuthority.add(new SimpleGrantedAuthority(authorityString));
        }
        return new User(username, password, grantedAuthority);
    }
    
}
