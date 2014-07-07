
package org.fosstrak.epcis.soap;

import javax.xml.ws.WebFault;


/**
 * This class was generated by Apache CXF 2.6.1
 * 2013-12-04T15:28:18.711+09:00
 * Generated source version: 2.6.1
 */

@WebFault(name = "QueryParameterException", targetNamespace = "urn:epcglobal:epcis-query:xsd:1")
public class QueryParameterExceptionResponse extends Exception {
    
    private org.fosstrak.epcis.model.QueryParameterException queryParameterException;

    public QueryParameterExceptionResponse() {
        super();
    }
    
    public QueryParameterExceptionResponse(String message) {
        super(message);
    }
    
    public QueryParameterExceptionResponse(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryParameterExceptionResponse(String message, org.fosstrak.epcis.model.QueryParameterException queryParameterException) {
        super(message);
        this.queryParameterException = queryParameterException;
    }

    public QueryParameterExceptionResponse(String message, org.fosstrak.epcis.model.QueryParameterException queryParameterException, Throwable cause) {
        super(message, cause);
        this.queryParameterException = queryParameterException;
    }

    public org.fosstrak.epcis.model.QueryParameterException getFaultInfo() {
        return this.queryParameterException;
    }
}
