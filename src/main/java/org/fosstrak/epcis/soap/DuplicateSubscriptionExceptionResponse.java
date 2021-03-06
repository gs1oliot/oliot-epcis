
package org.fosstrak.epcis.soap;

import javax.xml.ws.WebFault;


/**
 * This class was generated by Apache CXF 2.6.1
 * 2013-12-04T15:28:18.754+09:00
 * Generated source version: 2.6.1
 */

@WebFault(name = "DuplicateSubscriptionException", targetNamespace = "urn:epcglobal:epcis-query:xsd:1")
public class DuplicateSubscriptionExceptionResponse extends Exception {
    
    /**
     * 
     */
    private static final long serialVersionUID = 1L;
    private org.fosstrak.epcis.model.DuplicateSubscriptionException duplicateSubscriptionException;

    public DuplicateSubscriptionExceptionResponse() {
        super();
    }
    
    public DuplicateSubscriptionExceptionResponse(String message) {
        super(message);
    }
    
    public DuplicateSubscriptionExceptionResponse(String message, Throwable cause) {
        super(message, cause);
    }

    public DuplicateSubscriptionExceptionResponse(String message, org.fosstrak.epcis.model.DuplicateSubscriptionException duplicateSubscriptionException) {
        super(message);
        this.duplicateSubscriptionException = duplicateSubscriptionException;
    }

    public DuplicateSubscriptionExceptionResponse(String message, org.fosstrak.epcis.model.DuplicateSubscriptionException duplicateSubscriptionException, Throwable cause) {
        super(message, cause);
        this.duplicateSubscriptionException = duplicateSubscriptionException;
    }

    public org.fosstrak.epcis.model.DuplicateSubscriptionException getFaultInfo() {
        return this.duplicateSubscriptionException;
    }
}
