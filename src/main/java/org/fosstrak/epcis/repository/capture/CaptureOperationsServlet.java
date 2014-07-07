/*
 * Copyright (C) 2007 ETH Zurich
 *
 * This file is part of Fosstrak (www.fosstrak.org).
 *
 * Fosstrak is free software; you can redistribute it and/or
 * modify it under the terms of the GNU Lesser General Public
 * License version 2.1, as published by the Free Software Foundation.
 *
 * Fosstrak is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the GNU
 * Lesser General Public License for more details.
 *
 * You should have received a copy of the GNU Lesser General Public
 * License along with Fosstrak; if not, write to the Free
 * Software Foundation, Inc., 51 Franklin Street, Fifth Floor,
 * Boston, MA  02110-1301  USA
 */

package org.fosstrak.epcis.repository.capture;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.sql.SQLException;
import java.util.Properties;

import javax.servlet.RequestDispatcher;
import javax.servlet.ServletConfig;
import javax.servlet.ServletContext;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServlet;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.fosstrak.epcis.repository.CassandraResource;
import org.fosstrak.epcis.repository.InvalidFormatException;

import org.xml.sax.SAXException;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * This CaptureOperationsServlet accepts and analyzes HTTP POST requests and
 * delegates them to the appropriate handler methods in the
 * CaptureOperationsModule. This servlet also initializes the
 * CaptureOperationsModule properly and returns a simple information page upon
 * GET requests.
 * 
 * @author Marco Steybe
 */
public class CaptureOperationsServlet extends HttpServlet {

    private static final long serialVersionUID = -5765052834995535731L;

    private static final String APP_CONFIG_LOCATION = "appConfigLocation";
    private static final String PROP_INSERT_MISSING_VOC = "insertMissingVoc";
    private static final String PROP_DB_RESET_ALLOWED = "dbResetAllowed";
    private static final String PROP_DB_RESET_SCRIPT = "dbResetScript";
    private static final String PROP_EPCIS_SCHEMA_FILE = "epcisSchemaFile";
    private static final String PROP_EPCIS_MASTER_DATA_SCHEMA_FILE = "epcisMasterDataSchemaFile";
    private static final String GEO_DISCOVERY_ENABLE = "geoDiscoveryEnable";
    private static final String GEO_DISCOVERY_URL = "geoDiscoveryURL";
    private static final String GEO_DISCOVERY_PORT = "geoDiscoveryPORT";
    private static final String EPCIS_REPOSITORY_URL = "epcisRepositoryURL";

    private static final String PAGE_CAPTURE_INTERFACE = "/WEB-INF/jsp/capture.jsp";
    private static final String PAGE_CAPTURE_FORM = "/WEB-INF/jsp/captureForm.jsp";

    private static final Log LOG = LogFactory.getLog(CaptureOperationsServlet.class);

    private CaptureOperationsModule captureOperationsModule;
    
    private Cluster cluster;
    private Session session;
    private CassandraResource cassandraResource;

    /**
     * {@inheritDoc}
     */
    
    public void init() {
        LOG.debug("Fetching capture operations module from servlet context ...");
        CaptureOperationsModule captureOperationsModule = (CaptureOperationsModule) getServletContext().getAttribute(
                "captureOperationsModule");
        cassandraResource = (CassandraResource) getServletContext().getAttribute("CassandraBean");
        
        LOG.info("init Servlet");
        ServletConfig servletConfig = getServletConfig();
//        ServletContext context = servletConfig.getServletContext();
//       
//        String CassandraIP = context.getInitParameter("CassandraIP");
        LOG.info("Cassandra address: " + cassandraResource.getHosts());
        try {
        cluster = Cluster.builder().addContactPoint(cassandraResource.getHosts()).build();
        session = cluster.connect();
        session.execute("USE " + cassandraResource.getKeyspace());
        //captureOperationsModule.setCluster(cluster);
        captureOperationsModule.setSession(session);
        captureOperationsModule.setBackend(new CaptureOperationsBackendCassandra());
        } catch (NoHostAvailableException e) {
        	LOG.info("No hosts available");
        	LOG.info("No Cassandra host");
        }
        if (captureOperationsModule == null) {
            LOG.debug("Capture operations module not found - initializing manually");
            captureOperationsModule = new CaptureOperationsModule();
            Properties props;
            if (servletConfig == null) {
                props = loadApplicationProperties();
            } else {
                props = loadApplicationProperties(servletConfig);
            }
//            SessionFactory hibernateSessionFactory = initHibernate();
//            captureOperationsModule.setSessionFactory(hibernateSessionFactory);
                       
            captureOperationsModule.setInsertMissingVoc(Boolean.parseBoolean(props.getProperty(PROP_INSERT_MISSING_VOC,
                    "true")));
            captureOperationsModule.setDbResetAllowed(Boolean.parseBoolean(props.getProperty(PROP_DB_RESET_ALLOWED,
                    "false")));
            //captureOperationsModule.setDbResetScript(props.getProperty(PROP_DB_RESET_SCRIPT));
            captureOperationsModule.setEpcisSchemaFile(props.getProperty(PROP_EPCIS_SCHEMA_FILE));
            captureOperationsModule.setEpcisMasterdataSchemaFile(props.getProperty(PROP_EPCIS_MASTER_DATA_SCHEMA_FILE));            
        } else {
            LOG.debug("Capture operations module found");
        }
        setCaptureOperationsModule(captureOperationsModule);
    }

    /**
     * Loads the application properties and populates a java.util.Properties
     * instance.
     * 
     * @param servletConfig
     *            The ServletConfig used to locate the application property
     *            file.
     * @return The application properties.
     */
    private Properties loadApplicationProperties(ServletConfig servletConfig) {
        // read application properties from servlet context
        ServletContext ctx = servletConfig.getServletContext();
        String path = ctx.getRealPath("/");
        String appConfigFile = ctx.getInitParameter(APP_CONFIG_LOCATION);
        Properties properties = new Properties();
        try {
            InputStream is = new FileInputStream(path + appConfigFile);
            properties.load(is);
            is.close();
            LOG.info("Loaded application properties from " + path + appConfigFile);
        } catch (IOException e) {
            LOG.error("Unable to load application properties from " + path + appConfigFile, e);
        }
        return properties;
    }

    /**
     * Loads the application properties from classpath and populates a
     * java.util.Properties instance.
     * 
     * @param servletConfig
     *            The ServletConfig used to locate the application property
     *            file.
     * @return The application properties.
     */
    private Properties loadApplicationProperties() {
        // read application properties from classpath
        String resource = "/application.properties";
        InputStream is = this.getClass().getResourceAsStream(resource);
        Properties properties = new Properties();
        try {
            properties.load(is);
            is.close();
            LOG.info("Loaded application properties from classpath:" + resource + " ("
                    + this.getClass().getResource(resource) + ")");
        } catch (IOException e) {
            LOG.error("Unable to load application properties from classpath:" + resource + " ("
                    + this.getClass().getResource(resource) + ")", e);
        }
        return properties;
    }

    /**
     * Handles HTTP GET requests by dispatching to simple information pages.
     * 
     * @see javax.servlet.http.HttpServlet#doGet(javax.servlet.http.HttpServletRequest,
     *      javax.servlet.http.HttpServletResponse)
     * @param req
     *            The servlet request.
     * @param rsp
     *            The servlet response.
     * @throws IOException
     *             If an error occurred while writing the response.
     */
    public void doGet(final HttpServletRequest req, final HttpServletResponse rsp) throws ServletException, IOException {
    	LOG.info("doGet run");
    	RequestDispatcher dispatcher;
        String dbReset = req.getParameter("dbReset");
        if (dbReset != null && dbReset.equalsIgnoreCase("true")) {
            doDbReset(rsp);
        } else {
	        String showCaptureForm = req.getParameter("showCaptureForm");
	        if (showCaptureForm != null && "true".equals(showCaptureForm)) {
	            req.setAttribute("responseMsg", "");
	            req.setAttribute("detailedMsg", "");
	            dispatcher = getServletContext().getRequestDispatcher(PAGE_CAPTURE_FORM);
	        } else {
	            dispatcher = getServletContext().getRequestDispatcher(PAGE_CAPTURE_INTERFACE);
	        }
	        dispatcher.forward(req, rsp);
        }
    }

    /**
     * Implements the EPCIS capture operation. Takes HTTP POST request, extracts
     * the payload into an XML document, validates the document against the
     * EPCIS schema, and captures the EPCIS events given in the document. Errors
     * are caught and returned as simple plaintext messages via HTTP.
     * 
     * @param req
     *            The HttpServletRequest.
     * @param rsp
     *            The HttpServletResponse.
     * @throws IOException
     *             If an error occurred while validating the request or writing
     *             the response.
     */
    public void doPost(final HttpServletRequest req, final HttpServletResponse rsp) throws ServletException, IOException {
        LOG.info("EPCIS Capture Interface invoked.");
        InputStream is = null;
        
        // check if we have a POST request with form parameters
        if ("application/x-www-form-urlencoded".equalsIgnoreCase(req.getContentType())) {
            rsp.setContentType("text/plain");
            PrintWriter out = rsp.getWriter();
            // check if the 'event' or 'dbReset' form parameter are given
            String event = req.getParameter("event");
            String dbReset = req.getParameter("dbReset");
            if (event != null) {
                LOG.info("Found deprecated 'event=' parameter. Refusing to process request.");
                String msg = "Starting from version 0.2.2, the EPCIS repository does not accept the EPCISDocument in the HTTP POST form parameter 'event' anymore. Please provide the EPCISDocument as HTTP POST payload instead.";
                rsp.setStatus(HttpServletResponse.SC_NOT_ACCEPTABLE);
                out.println(msg);
            } else if (dbReset != null && dbReset.equalsIgnoreCase("true")) {
                doDbReset(rsp);
            }
            out.flush();
            out.close();
            return;
        } else {
            is = req.getInputStream();
        }
        
        // do the capture operation and handle exceptions
        String responseMsg = "";
        String detailedMsg = "";
        try {
            captureOperationsModule.doCapture(is, req.getUserPrincipal());
            rsp.setStatus(HttpServletResponse.SC_OK);
            responseMsg = "EPCIS capture request succeeded.";
        } catch (SAXException e) {
            responseMsg = "An error processing the XML document occurred.";
            detailedMsg = "Unable to parse incoming XML due to error: " + e.getMessage();
            LOG.info(detailedMsg);
            rsp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } catch (InvalidFormatException e) {
            responseMsg = "An error parsing the XML contents occurred.";
            detailedMsg = "Unable to parse incoming EPCISDocument due to error: " + e.getMessage();
            LOG.info(detailedMsg);
            rsp.setStatus(HttpServletResponse.SC_BAD_REQUEST);
        } catch (final Exception e) {
            responseMsg = "An unexpected error occurred.";
            detailedMsg = "The repository is unable to handle the request due to an internal error. " + e.getMessage();
            LOG.error(responseMsg, e);
            rsp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
        }

        // dispatch the response
        req.setAttribute("responseMsg", responseMsg);
        req.setAttribute("detailedMsg", detailedMsg);
        RequestDispatcher dispatcher;
        String showCaptureForm = (String) req.getAttribute("showCaptureForm");
        if (showCaptureForm != null && "true".equals(showCaptureForm)) {
            dispatcher = getServletContext().getRequestDispatcher(PAGE_CAPTURE_FORM);
        } else {
            dispatcher = getServletContext().getRequestDispatcher(PAGE_CAPTURE_INTERFACE);
        }
        dispatcher.forward(req, rsp);
    }

    private void doDbReset(final HttpServletResponse rsp) throws IOException {
        LOG.debug("Found 'dbReset' parameter set to 'true'.");
        rsp.setContentType("text/plain");
        final PrintWriter out = rsp.getWriter();
//        try {
//            captureOperationsModule.doDbReset();
//            String msg = "db reset successfull";
//            LOG.info(msg);
//            rsp.setStatus(HttpServletResponse.SC_OK);
//            out.println(msg);
//        } catch (SQLException e) {
//            String msg = "An error involving the database occurred";
//            LOG.error(msg, e);
//            rsp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
//            out.println(msg);
//        } catch (IOException e) {
//            String msg = "An unexpected error occurred";
//            LOG.error(msg, e);
//            rsp.setStatus(HttpServletResponse.SC_INTERNAL_SERVER_ERROR);
//            out.println(msg);
//        } catch (UnsupportedOperationException e) {
//            String msg = "'dbReset' operation not allowed!";
//            LOG.info(msg);
//            rsp.setStatus(HttpServletResponse.SC_FORBIDDEN);
//            out.println(msg);
//        }
    }

    public void setCaptureOperationsModule(CaptureOperationsModule captureOperationsModule) {
        this.captureOperationsModule = captureOperationsModule;
    }
    
    public void destroy() {
    	try {
    		if (cluster != null) {
    	          cluster.shutdown();
    	          LOG.info("shutdown cluster");
    		}
    	} catch (Exception e) {
    		e.printStackTrace();
    	}
    }
}
