package org.datakow.apps.objectcatalogwebservice.exception;

import org.springframework.http.HttpStatus;

/**
 *
 * @author kevin.off
 */
public class UnauthorizedException extends WebserviceException{
    
    public UnauthorizedException(String realm) {
        super(makeMessage(realm));
    }
    
    
    public static final String makeMessage(String realm){
        return "You are not authorized to access the security realm " + realm;
    }

    @Override
    public String getCode() {
        return "FORBIDDEN";
    }

    @Override
    public boolean shouldLog() {
        return false;
    }

    @Override
    public HttpStatus getHttpStatus() {
        return HttpStatus.FORBIDDEN;
    }
    
}
