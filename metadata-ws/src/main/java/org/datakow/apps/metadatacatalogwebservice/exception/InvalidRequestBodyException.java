package org.datakow.apps.metadatacatalogwebservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;

/**
 *
 * @author kevin.off
 */
public class InvalidRequestBodyException extends WebserviceException {
    
    public InvalidRequestBodyException(){
        super(makeMessage(null));
    }
    
    public InvalidRequestBodyException(String detail) {
        super(makeMessage(detail));
    }
    
    public InvalidRequestBodyException(String detail, Throwable cause) {
        super(makeMessage(detail), cause);
    }
 
    public static final String makeMessage(String detail){
        StringBuilder builder = new StringBuilder("The request body is invalid.");
        if (StringUtils.hasText(detail)){
            builder.append(" ").append(detail);
        }
        return builder.toString();
    }

    @Override
    public String getCode() {
        return "BAD_REQUEST_BODY";
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
