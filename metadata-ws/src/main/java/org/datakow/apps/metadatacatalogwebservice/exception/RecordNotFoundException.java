package org.datakow.apps.metadatacatalogwebservice.exception;

import org.datakow.core.components.CatalogIdentity;
import org.springframework.http.HttpStatus;
import org.springframework.util.StringUtils;

/**
 *
 * @author kevin.off
 */
public class RecordNotFoundException extends WebserviceException {
    
    public RecordNotFoundException(){
        super("No records could be found to perform the operation");
    }
    
    public RecordNotFoundException(CatalogIdentity identity) {
        super(makeMessage(identity, null));
    }
    
    public RecordNotFoundException(CatalogIdentity identity, String detail){
        super(makeMessage(identity, detail));
    }
    
    public static final String makeMessage(CatalogIdentity identity, String detail){
        StringBuilder builder = new StringBuilder("The record ")
                .append(identity.getRecordIdentifier())
                .append(" does not exist in the ")
                .append(identity.getCatalogIdentifier())
                .append(" catalog.");
        if (StringUtils.hasText(detail)){
            builder.append(" ").append(detail);
        }
        return builder.toString();
    }

    @Override
    public String getCode() {
        return "RECORD_NOT_FOUND";
    }

    @Override
    public boolean shouldLog() {
        return false;
    }
    
    @Override
    public HttpStatus getHttpStatus() {
        return HttpStatus.NOT_FOUND;
    }
    
}
