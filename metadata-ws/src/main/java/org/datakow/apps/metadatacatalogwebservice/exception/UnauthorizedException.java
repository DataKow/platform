package org.datakow.apps.metadatacatalogwebservice.exception;

import org.springframework.http.HttpStatus;

/**
 *
 * @author kevin.off
 */
public class UnauthorizedException extends WebserviceException{
    
    public UnauthorizedException() {
        super(makeMessage("You are unauthorized to access this resource"));
    }
    
    public UnauthorizedException(String resource) {
        super(makeMessage(resource));
    }
    
    
    public static final String makeMessage(String resource){
        return "You are not authorized to access this resource: " + resource;
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
