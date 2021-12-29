package org.datakow.apps.subscriptionwebservice;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.mongodb.DuplicateKeyException;
import com.mongodb.MongoException;
import cz.jirutka.rsql.parser.RSQLParserException;
import org.datakow.apps.subscriptionwebservice.exception.WebserviceException;
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
import org.springframework.web.client.RestClientResponseException;

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
        headers.setContentType(MediaType.APPLICATION_JSON);
        return new ResponseEntity<>(buildErrorMessage(ex.getCode(), ex.getLocalizedMessage()), headers, ex.getHttpStatus());
    }
    
    @ExceptionHandler(DuplicateKeyException.class)
    @ResponseStatus(HttpStatus.CONFLICT)
    public String mongoDuplicateKeyExceptionHandler(DuplicateKeyException ex){
        return buildErrorMessage("DUPLICATE_KEY", "Duplicate unique index constraint.");
    }
    
    @ExceptionHandler(MongoException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public String mongoExceptionHandler(MongoException ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "A Mongo Exception occurred", ex);
        return buildErrorMessage("DATABASE_ERROR", "There was an error communicating with the database.");
    }
    
    @ExceptionHandler(JsonProcessingException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String jsonProcessingExceptionHandler(JsonProcessingException ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "A JSON Processing Exception occurred", ex);
        return buildErrorMessage("JSON_PARSE_ERROR", "There was an error parsing JSON in your request or in the response.");
    }
    
    @ExceptionHandler(IOException.class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    public String ioExceptionHandler(IOException ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "An IOException occurred", ex);
        return buildErrorMessage("NETWORK_ERROR", "A network communication error occurred.");
    }
    
    @ExceptionHandler(RSQLParserException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    public String rsqlParseExceptionHandler(RSQLParserException ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "A RSQL parser exception occurred", ex);
        return buildErrorMessage("FIQL_PARSE_ERROR", "There was an error parsing the FIQL query in your request.");
    }
    
    @ExceptionHandler(HttpMediaTypeException.class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
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
    
    @ExceptionHandler(RestClientResponseException.class)
    public ResponseEntity<String> restClientExceptionHandler(RestClientResponseException ex){
        Logger.getLogger(CustomExceptionHandler.class.getName()).log(Level.SEVERE, "A Rest Client Exception Occurred", ex);
        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON_UTF8);
        HttpStatus status = HttpStatus.valueOf(ex.getRawStatusCode());
        return new ResponseEntity(buildErrorMessage(status.toString(), status.getReasonPhrase()), headers, status);
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
