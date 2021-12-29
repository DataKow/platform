package org.datakow.apps.metadatacatalogwebservice.exception;

import org.springframework.http.HttpStatus;

/**
 *
 * @author kevin.off
 */
public class CatalogDoesNotExistException extends WebserviceException {
    
    
    public CatalogDoesNotExistException(String catalogIdentifier, Throwable cause) {
        super(makeMessage(catalogIdentifier), cause);
    }
    
    public CatalogDoesNotExistException(String catalogIdentifier) {
        super(makeMessage(catalogIdentifier));
    }
    
    public static final String makeMessage(String catalogIdentifier){
        return "The catalog " + catalogIdentifier + " does not exist.";
    }

    @Override
    public String getCode() {
        return "CATALOG_DOES_NOT_EXIST";
    }

    @Override
    public boolean shouldLog() {
        return false;
    }

    @Override
    public HttpStatus getHttpStatus() {
        return HttpStatus.BAD_REQUEST;
    }
    
}
