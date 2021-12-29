package org.datakow.apps.objectcatalogwebservice;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoException;
import cz.jirutka.rsql.parser.RSQLParserException;
import org.datakow.apps.objectcatalogwebservice.exception.WebserviceException;
import java.io.IOException;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.logging.log4j.ThreadContext;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.HttpMediaTypeException;
import org.springframework.web.HttpRequestMethodNotSupportedException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseStatus;
import org.springframework.web.bind.annotation.RestControllerAdvice;

/**
 *
 * @author kevin.off
 */
@RestControllerAdvice
public class CustomExceptionHandler {
    
    @ExceptionHandler(WebserviceException.class)
    public ResponseEntity<String> defaultWebserviceExceptionHandler(WebserviceException ex){
        if (ex.shouldLog()){
            Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "An exception occurred", ex);
        }
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        return new ResponseEntity<>(buildErrorMessage(ex.getCode(), ex.getLocalizedMessage()), headers, ex.getHttpStatus());
    }
    
    @ExceptionHandler(DuplicateKeyException.class)
    public ResponseEntity mongoDuplicateKeyExceptionHandler(DuplicateKeyException ex){
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        return new ResponseEntity<>(buildErrorMessage("DUPLICATE_KEY", "Duplicate unique index constraint."), headers, HttpStatus.CONFLICT);
    }
    
    @ExceptionHandler(MongoException.class)
    public ResponseEntity mongoExceptionHandler(MongoException ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "A Mongo Exception occurred", ex);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        return new ResponseEntity<>(buildErrorMessage("DATABASE_ERROR", 
                "There was an error communicating with the database."), headers, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    
    @ExceptionHandler()
    public ResponseEntity jsonProcessingExceptionHandler(JsonProcessingException ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "A JSON Processing Exception occurred", ex);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        return new ResponseEntity<>(buildErrorMessage("JSON_PARSE_ERROR", 
                "There was an error parsing JSON during your request."), headers, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    
    @ExceptionHandler(IOException.class)
    public ResponseEntity ioExceptionHandler(IOException ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "An IOException occurred", ex);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        return new ResponseEntity<>(buildErrorMessage("NETWORK_ERROR", "A network communication error occurred."), headers, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    
    @ExceptionHandler(RSQLParserException.class)
    public ResponseEntity rsqlParseExceptionHandling(RSQLParserException ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "A RSQL parser exception occurred", ex);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        return new ResponseEntity<>(buildErrorMessage("FIQL_PARSE_ERROR", "There was an error parsing the FIQL query in your request."), headers, HttpStatus.BAD_REQUEST);
    }
    
    @ExceptionHandler(HttpMediaTypeException.class)
    public ResponseEntity<String> mediaTypeException(HttpMediaTypeException ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "An unknown exception occurred", ex);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        return new ResponseEntity(buildErrorMessage("MEDIA_TYPE_EXCEPTION", ex.getLocalizedMessage()), headers, HttpStatus.BAD_REQUEST);
    }
    
    @ExceptionHandler(HttpRequestMethodNotSupportedException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public ResponseEntity<String> methodNotSupportedHandler(HttpRequestMethodNotSupportedException ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "An unknown exception occurred", ex);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        return new ResponseEntity(buildErrorMessage("METHOD_NOT_SUPPORTED", ex.getLocalizedMessage()), headers, HttpStatus.BAD_REQUEST);
    }
    
    @ExceptionHandler(Throwable.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public ResponseEntity<String> defaultExceptionHandler(Throwable ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "An unknown exception occurred", ex);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        return new ResponseEntity(buildErrorMessage("UNKNOWN_EXCEPTION", "There was an unknown exception during your request."), headers, HttpStatus.INTERNAL_SERVER_ERROR);
    }
    
    public static String buildErrorMessage(String code, String message){
        StringBuilder builder = new StringBuilder();
        builder.append("{\"code\"").append(":").append("\"").append(code).append("\",");
        builder.append("\"message\"").append(":").append("\"").append(message).append("\",");
        builder.append("\"correlationId\"").append(":").append("\"").append(ThreadContext.get("correlationId")).append("\"").append(",");
        builder.append("\"requestId\"").append(":").append("\"").append(ThreadContext.get("requestId")).append("\"}");
        return builder.toString();
    }
}
