package org.datakow.apps.subscriptionwebservice.exception;

import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ResponseStatus;

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
        return "The request parameter " + parameterName + " is missing or the value is invalid.";
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
