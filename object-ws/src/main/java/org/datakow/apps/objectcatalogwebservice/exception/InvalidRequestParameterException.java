package org.datakow.apps.objectcatalogwebservice.exception;

import org.springframework.http.HttpStatus;

/**
 *
 * @author kevin.off
 */
public class InvalidRequestParameterException extends WebserviceException{
    
    public InvalidRequestParameterException(String parameterName, String detail){
        super(makeMessage(parameterName, detail));
    }
    
    public InvalidRequestParameterException(String parameterName) {
        super(makeMessage(parameterName));
    }
    
    public InvalidRequestParameterException(String parameterName, Throwable cause) {
        super(makeMessage(parameterName), cause);
    }
    
    public static final String makeMessage(String parameterName){
        return "The request parameter(s) " + parameterName + " is/are missing or the value is invalid.";
    }
    
    public static final String makeMessage(String parameterName, String detail){
        return makeMessage(parameterName) + detail;
    }

    @Override
    public String getCode() {
        return "BAD_REQUEST_PARAMETER";
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
