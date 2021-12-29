package org.datakow.apps.objectcatalogwebservice.exception;

import org.springframework.http.HttpStatus;

/**
 *
 * @author kevin.off
 */
public class SourceRecordNotFoundException extends WebserviceException{
    
    public SourceRecordNotFoundException(String sourceRecordIdentifier) {
        super(makeMessage(sourceRecordIdentifier));
    }
    
    
    public static final String makeMessage(String sourceRecordIdentifier){
        return "The source record " + sourceRecordIdentifier + " cannot be found.";
    }

    @Override
    public String getCode() {
        return "OBJECT_GONE";
    }

    @Override
    public boolean shouldLog() {
        return false;
    }

    @Override
    public HttpStatus getHttpStatus() {
        return HttpStatus.GONE;
    }
    
}
