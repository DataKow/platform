package org.datakow.apps.objectcatalogwebservice.exception;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.datakow.apps.objectcatalogwebservice.CustomExceptionHandler;
import org.springframework.core.NestedRuntimeException;
import org.springframework.http.HttpStatus;

/**
 *
 * @author kevin.off
 */
public abstract class WebserviceException extends NestedRuntimeException{
    
    String message;
    
    public WebserviceException(String msg) {
        super(msg);
        this.message = msg;
    }
    
    public WebserviceException(String msg, Throwable cause) {
        super(msg, cause);
        this.message = msg;
    }
    
    @JsonIgnore
    @Override
    public String getLocalizedMessage(){
        return message;
    }
    
    public abstract HttpStatus getHttpStatus();
    
    public abstract String getCode();
    
    public abstract boolean shouldLog();
    
    public String toJson(){
        return CustomExceptionHandler.buildErrorMessage(this.getCode(), this.getLocalizedMessage());
    }
    
}
