package org.fosstrak.epcis.repository;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
/**
 * CassandraResource establishes Cassandra sessions for communicating between the EPCIS and Cassandra cluster. 
 * @author Nicholasle
 *
 */
public class CassandraResource {
	private static final Log LOG = LogFactory.getLog(CassandraResource.class);
    private String hosts;
    private String keyspace;
    private String username;
    private String password;
    private Cluster cluster;
    private Session session;
    
    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    public String getHosts() {
        return hosts;
    }

    public void setHosts(String hosts) {
        this.hosts = hosts;
    }

    public String getKeyspace() {
        return keyspace;
    }

    public void setKeyspace(String keyspace) {
        this.keyspace = keyspace;
    }

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }    
    
    public Session createOrGetSession() {
        try {
            if (cluster == null) {
                cluster = Cluster.builder().addContactPoint(hosts).build();
                LOG.info("Cluster is Null");
            }
            
            if (session == null) {
                session = cluster.connect();
                session.execute("USE " + keyspace);
                LOG.info("Session is NULL. Create session successfully from Cassandra Resource");
            } else {
                LOG.info("Reuse Session!");
            }
        } catch (NoHostAvailableException e) {
            LOG.info("No Cassandra host");
        } catch (Exception e) {
            e.printStackTrace();
        }
        
        return session;
    }
    
    public void cleanUp() {
        if (cluster != null) {
            LOG.info("Cassandra cluster shutdown!. Injected from Spring bean");
            cluster.shutdown();
            
        }
    }
    

}
