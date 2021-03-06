package org.fosstrak.epcis.soap;

import java.net.MalformedURLException;
import java.net.URL;
import javax.xml.namespace.QName;
import javax.xml.ws.WebEndpoint;
import javax.xml.ws.WebServiceClient;
import javax.xml.ws.WebServiceFeature;
import javax.xml.ws.Service;

/**
 * This class was generated by Apache CXF 2.6.1
 * 2013-12-04T15:28:18.779+09:00
 * Generated source version: 2.6.1
 * 
 */
@WebServiceClient(name = "EPCglobalEPCISService", 
                  wsdlLocation = "file:/D:/Fosstrak%20EPCIS_Dec/epcis-commons/../epcis-repository/src/main/resources/wsdl/EPCglobal-epcis-query-1_0.wsdl",
                  targetNamespace = "urn:epcglobal:epcis:wsdl:1") 
public class EPCglobalEPCISService extends Service {

    public final static URL WSDL_LOCATION;

    public final static QName SERVICE = new QName("urn:epcglobal:epcis:wsdl:1", "EPCglobalEPCISService");
    public final static QName EPCglobalEPCISServicePort = new QName("urn:epcglobal:epcis:wsdl:1", "EPCglobalEPCISServicePort");
    static {
        URL url = null;
        try {
            url = new URL("file:/D:/Fosstrak%20EPCIS_Dec/epcis-commons/../epcis-repository/src/main/resources/wsdl/EPCglobal-epcis-query-1_0.wsdl");
        } catch (MalformedURLException e) {
            java.util.logging.Logger.getLogger(EPCglobalEPCISService.class.getName())
                .log(java.util.logging.Level.INFO, 
                     "Can not initialize the default wsdl from {0}", "file:/D:/Fosstrak%20EPCIS_Dec/epcis-commons/../epcis-repository/src/main/resources/wsdl/EPCglobal-epcis-query-1_0.wsdl");
        }
        WSDL_LOCATION = url;
    }

    public EPCglobalEPCISService(URL wsdlLocation) {
        super(wsdlLocation, SERVICE);
    }

    public EPCglobalEPCISService(URL wsdlLocation, QName serviceName) {
        super(wsdlLocation, serviceName);
    }

    public EPCglobalEPCISService() {
        super(WSDL_LOCATION, SERVICE);
    }
    
    //This constructor requires JAX-WS API 2.2. You will need to endorse the 2.2
    //API jar or re-run wsdl2java with "-frontend jaxws21" to generate JAX-WS 2.1
    //compliant code instead.
    public EPCglobalEPCISService(WebServiceFeature ... features) {
        super(WSDL_LOCATION, SERVICE, features);
    }

    //This constructor requires JAX-WS API 2.2. You will need to endorse the 2.2
    //API jar or re-run wsdl2java with "-frontend jaxws21" to generate JAX-WS 2.1
    //compliant code instead.
    public EPCglobalEPCISService(URL wsdlLocation, WebServiceFeature ... features) {
        super(wsdlLocation, SERVICE, features);
    }

    //This constructor requires JAX-WS API 2.2. You will need to endorse the 2.2
    //API jar or re-run wsdl2java with "-frontend jaxws21" to generate JAX-WS 2.1
    //compliant code instead.
    public EPCglobalEPCISService(URL wsdlLocation, QName serviceName, WebServiceFeature ... features) {
        super(wsdlLocation, serviceName, features);
    }

    /**
     *
     * @return
     *     returns EPCISServicePortType
     */
    @WebEndpoint(name = "EPCglobalEPCISServicePort")
    public EPCISServicePortType getEPCglobalEPCISServicePort() {
        return super.getPort(EPCglobalEPCISServicePort, EPCISServicePortType.class);
    }

    /**
     * 
     * @param features
     *     A list of {@link javax.xml.ws.WebServiceFeature} to configure on the proxy.  Supported features not in the <code>features</code> parameter will have their default values.
     * @return
     *     returns EPCISServicePortType
     */
    @WebEndpoint(name = "EPCglobalEPCISServicePort")
    public EPCISServicePortType getEPCglobalEPCISServicePort(WebServiceFeature... features) {
        return super.getPort(EPCglobalEPCISServicePort, EPCISServicePortType.class, features);
    }

}
