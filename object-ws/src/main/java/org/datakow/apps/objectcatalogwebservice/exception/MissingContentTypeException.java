package org.datakow.apps.objectcatalogwebservice.exception;

import org.springframework.http.HttpStatus;

/**
 *
 * @author kevin.off
 */
public class MissingContentTypeException extends WebserviceException{
    
    public MissingContentTypeException(){
        super(makeMessage());
    }
    
    public MissingContentTypeException(String detail) {
        super(makeMessage(detail));
    }
    
    
    public static final String makeMessage(){
        return "You are required to include a Content-Type header with your request.";
    }
    
    public static final String makeMessage(String detail){
        return makeMessage() + " " + detail;
    }

    @Override
    public String getCode() {
        return "MISSING_CONTENT_TYPE";
    }

    @Override
    public boolean shouldLog() {
        return false;
    }

    @Override
    public HttpStatus getHttpStatus() {
        return HttpStatus.UNSUPPORTED_MEDIA_TYPE;
    }
    
}
