package org.oliot.geodiscoveryservice;

import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.oliot.geodiscoveryservice.Registration_ES;
import com.google.gson.Gson;

/**
 * This is a client used to access discovery service.
 * 
 * @author Kiwoong
 *
 */
public class ESclient {
    private String dsServiceURL;
    private int dsServicePort;

    public ESclient (String dsServiceURL, int dsServicePort) {
        this.dsServicePort = dsServicePort;
        this.dsServiceURL = dsServiceURL;
    }
    public void Send_Registration(String EPC, String EPC_Addr, String cur_time, double lat, double lon) {
        Client client =  new TransportClient()
            .addTransportAddress(new InetSocketTransportAddress(dsServiceURL, dsServicePort));
        
        Registration_ES reg = new Registration_ES();
        reg.EPC = EPC;
        reg.EPCIS = EPC_Addr;
        reg.timestamp = cur_time;
        reg.location.lat = lat;
        reg.location.lon = lon;
        
        Gson gson = new Gson();
        String json = gson.toJson(reg);
        client.prepareIndex("discoveryservice", "geo")
                .setSource(json)
                .execute()
                .actionGet();
        client.close();
    }
}

class Registration_ES {
    String EPC = new String();
    String EPCIS = new String();
    String timestamp = new String();
    Location_ES location = new Location_ES();
}

class Location_ES {
    double lat ;
    double lon ;
}


