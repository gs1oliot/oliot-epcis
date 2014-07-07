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

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.StringWriter;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.security.Principal;
import java.sql.SQLException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.xml.XMLConstants;
import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;
import javax.xml.parsers.ParserConfigurationException;
import javax.xml.transform.OutputKeys;
import javax.xml.transform.Source;
import javax.xml.transform.Transformer;
import javax.xml.transform.TransformerFactory;
import javax.xml.transform.dom.DOMSource;
import javax.xml.transform.stream.StreamResult;
import javax.xml.transform.stream.StreamSource;
import javax.xml.validation.Schema;
import javax.xml.validation.SchemaFactory;
import javax.xml.validation.Validator;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.elasticsearch.client.transport.NoNodeAvailableException;
import org.fosstrak.epcis.repository.EpcisConstants;
import org.fosstrak.epcis.repository.InternalBusinessException;
import org.fosstrak.epcis.repository.InvalidFormatException;
import org.fosstrak.epcis.repository.model.Action;
import org.fosstrak.epcis.repository.model.AggregationEvent;
import org.fosstrak.epcis.repository.model.BaseEvent;
import org.fosstrak.epcis.repository.model.BusinessLocationAttrId;
import org.fosstrak.epcis.repository.model.BusinessLocationId;
import org.fosstrak.epcis.repository.model.BusinessStepAttrId;
import org.fosstrak.epcis.repository.model.BusinessStepId;
import org.fosstrak.epcis.repository.model.BusinessTransaction;
import org.fosstrak.epcis.repository.model.BusinessTransactionAttrId;
import org.fosstrak.epcis.repository.model.BusinessTransactionId;
import org.fosstrak.epcis.repository.model.BusinessTransactionTypeAttrId;
import org.fosstrak.epcis.repository.model.BusinessTransactionTypeId;
import org.fosstrak.epcis.repository.model.DispositionAttrId;
import org.fosstrak.epcis.repository.model.DispositionId;
import org.fosstrak.epcis.repository.model.EPCClass;
import org.fosstrak.epcis.repository.model.EPCClassAttrId;
import org.fosstrak.epcis.repository.model.EventFieldExtension;
import org.fosstrak.epcis.repository.model.ObjectEvent;
import org.fosstrak.epcis.repository.model.QuantityEvent;
import org.fosstrak.epcis.repository.model.ReadPointAttrId;
import org.fosstrak.epcis.repository.model.ReadPointId;
import org.fosstrak.epcis.repository.model.TransactionEvent;
import org.fosstrak.epcis.repository.model.VocabularyAttrCiD;
import org.fosstrak.epcis.repository.model.VocabularyAttributeElement;
import org.fosstrak.epcis.repository.model.VocabularyElement;
import org.fosstrak.epcis.utils.TimeParser;

import org.oliot.geodiscoveryservice.ESclient;
import org.w3c.dom.DOMException;
import org.w3c.dom.Document;
import org.w3c.dom.Element;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.ErrorHandler;
import org.xml.sax.SAXException;
import org.xml.sax.SAXParseException;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;

/**
 * CaptureOperationsModule implements the core capture operations. Converts XML
 * events delivered by HTTP POST into SQL and inserts them into the database.
 * <p>
 * TODO: the parsing of the xml inputstream should be done in the
 * CaptureOperationsServlet; this class should implement EpcisCaptureInterface
 * such that CaptureOperationsServlet can call its capture method and provide it
 * with the parsed events.
 * 
 * @author David Gubler
 * @author Alain Remund
 * @author Marco Steybe
 * @author Nikos Kefalakis (nkef)
 * @author Tuan Le
 */
public class CaptureOperationsModule {

    private static final Log LOG = LogFactory.getLog(CaptureOperationsModule.class);

    private static final Map<String, Class<?>> vocClassMap = new HashMap<String, Class<?>>();

    static {
        vocClassMap.put(EpcisConstants.BUSINESS_LOCATION_ID, BusinessLocationId.class);
        vocClassMap.put(EpcisConstants.BUSINESS_STEP_ID, BusinessStepId.class);
        vocClassMap.put(EpcisConstants.BUSINESS_TRANSACTION_ID, BusinessTransactionId.class);
        vocClassMap.put(EpcisConstants.BUSINESS_TRANSACTION_TYPE_ID, BusinessTransactionTypeId.class);
        vocClassMap.put(EpcisConstants.DISPOSITION_ID, DispositionId.class);
        vocClassMap.put(EpcisConstants.EPC_CLASS_ID, EPCClass.class);
        vocClassMap.put(EpcisConstants.READ_POINT_ID, ReadPointId.class);
    }

    // (nkef) Added to support the Master Data Capture I/F
    private static final Map<String, Class<?>> vocAttributeClassMap = new HashMap<String, Class<?>>();
    private static final Map<String, String> vocAttributeTablesMap = new HashMap<String, String>();
    private static Map<String, String> vocabularyTablenameMap = new HashMap<String, String>();

    static {
        vocAttributeClassMap.put(EpcisConstants.BUSINESS_LOCATION_ID, BusinessLocationAttrId.class);
        vocAttributeClassMap.put(EpcisConstants.BUSINESS_STEP_ID, BusinessStepAttrId.class);
        vocAttributeClassMap.put(EpcisConstants.BUSINESS_TRANSACTION_ID, BusinessTransactionAttrId.class);
        vocAttributeClassMap.put(EpcisConstants.BUSINESS_TRANSACTION_TYPE_ID, BusinessTransactionTypeAttrId.class);
        vocAttributeClassMap.put(EpcisConstants.DISPOSITION_ID, DispositionAttrId.class);
        vocAttributeClassMap.put(EpcisConstants.EPC_CLASS_ID, EPCClassAttrId.class);
        vocAttributeClassMap.put(EpcisConstants.READ_POINT_ID, ReadPointAttrId.class);

        vocAttributeTablesMap.put(EpcisConstants.BUSINESS_LOCATION_ID, "voc_BizLoc_attr");
        vocAttributeTablesMap.put(EpcisConstants.BUSINESS_STEP_ID, "voc_BizStep_attr");
        vocAttributeTablesMap.put(EpcisConstants.BUSINESS_TRANSACTION_ID, "voc_BizTrans_attr");
        vocAttributeTablesMap.put(EpcisConstants.BUSINESS_TRANSACTION_TYPE_ID, "voc_BizTransType_attr");
        vocAttributeTablesMap.put(EpcisConstants.DISPOSITION_ID, "voc_Disposition_attr");
        vocAttributeTablesMap.put(EpcisConstants.EPC_CLASS_ID, "voc_EPCClass_attr");
        vocAttributeTablesMap.put(EpcisConstants.READ_POINT_ID, "voc_ReadPoint_attr");

        vocabularyTablenameMap = new HashMap<String, String>(8);
        vocabularyTablenameMap.put(EpcisConstants.BUSINESS_STEP_ID, "voc_BizStep");
        vocabularyTablenameMap.put(EpcisConstants.BUSINESS_LOCATION_ID, "voc_BizLoc");
        vocabularyTablenameMap.put(EpcisConstants.BUSINESS_TRANSACTION_ID, "voc_BizTrans");
        vocabularyTablenameMap.put(EpcisConstants.BUSINESS_TRANSACTION_TYPE_ID, "voc_BizTransType");
        vocabularyTablenameMap.put(EpcisConstants.DISPOSITION_ID, "voc_Disposition");
        vocabularyTablenameMap.put(EpcisConstants.EPC_CLASS_ID, "voc_EPCClass");
        vocabularyTablenameMap.put(EpcisConstants.READ_POINT_ID, "voc_ReadPoint");
        vocabularyTablenameMap.put(EpcisConstants.EPC_ID, "voc_EPC");

    }

    /**
     * The XSD schema which validates the incoming messages.
     */
    private Schema schema;

    /**
     * The XSD schema which validates the MasterData incoming messages.(nkef)
     */
    private Schema masterDataSchema;

    /**
     * Whether we should insert new vocabulary or throw an error message.
     */
    private boolean insertMissingVoc = true;

    /**
     * Whether the dbReset operation is allowed or not.
     */
    private boolean dbResetAllowed = false;

    /**
     * Interface to the database.
     */
    private CaptureOperationsBackendCassandra backend;
    private Cluster cluster;
    private Session session;
        
    /* For GeoDiscovery Service */
    private String geoDiscoveryURL;
    private int geoDiscoveryPORT;
    private String epcisRepositoryURL;
    private boolean geoDiscoveryEnable;
    private ESclient esClient;
    

    public String getGeoDiscoveryURL() {
        return geoDiscoveryURL;
    }

    public void setGeoDiscoveryURL(String geoDiscoveryURL) {
        this.geoDiscoveryURL = geoDiscoveryURL;
    }

    public int getGeoDiscoveryPORT() {
        return geoDiscoveryPORT;
    }

    public void setGeoDiscoveryPORT(int geoDiscoveryPORT) {
        this.geoDiscoveryPORT = geoDiscoveryPORT;
    }

    public String getEpcisRepositoryURL() {
        return epcisRepositoryURL;
    }

    public void setEpcisRepositoryURL(String epcisRepositoryURL) {
        this.epcisRepositoryURL = epcisRepositoryURL;
    }

    public boolean isGeoDiscoveryEnable() {
        return geoDiscoveryEnable;
    }

    public void setGeoDiscoveryEnable(boolean geoDiscoveryEnable) {
        this.geoDiscoveryEnable = geoDiscoveryEnable;
    }

    
    /* End of instance variable for GeoDiscovery Service */


    public Session getSession() {
        return session;
    }

    public void setSession(Session session) {
        this.session = session;
    }

    public Cluster getCluster() {
        return cluster;
    }

    public void setCluster(Cluster cluster) {
        this.cluster = cluster;
    }

    /**
     * Initializes the EPCIS schema used for validating incoming capture
     * requests. Loads the WSDL and XSD files from the classpath (the schema is
     * bundled with epcis-commons.jar).
     * 
     * @return An instantiated schema validation object.
     */
    private Schema initEpcisSchema(String xsdFile) {
        InputStream is = this.getClass().getResourceAsStream(xsdFile);
        if (is != null) {
            try {
                SchemaFactory schemaFactory = SchemaFactory.newInstance(XMLConstants.W3C_XML_SCHEMA_NS_URI);
                Source schemaSrc = new StreamSource(is);
                schemaSrc.setSystemId(CaptureOperationsServlet.class.getResource(xsdFile).toString());
                Schema schema = schemaFactory.newSchema(schemaSrc);
                LOG.debug("EPCIS schema file initialized and loaded successfully");
                return schema;
            } catch (Exception e) {
                LOG.warn("Unable to load or parse the EPCIS schema", e);
            }
        } else {
            LOG.error("Unable to load the EPCIS schema file from classpath: cannot find resource " + xsdFile);
        }
        LOG.warn("Schema validation will not be available!");
        return null;
    }    

    /**
     * Implements the EPCIS capture operation. Takes an input stream, extracts
     * the payload into an XML document, validates the document against the
     * EPCIS schema, and captures the EPCIS events given in the document.
     * 
     * @throws IOException
     *             If an error occurred while validating the request or writing
     *             the response.
     * @throws ParserConfigurationException
     * @throws SAXException
     *             If the XML document is malformed or invalid
     * @throws InvalidFormatException
     */
    public void doCapture(InputStream in, Principal principal) throws SAXException, InternalBusinessException,
            InvalidFormatException, NoHostAvailableException {
        if (esClient == null) {
            esClient = new ESclient(geoDiscoveryURL, geoDiscoveryPORT);
            LOG.debug("Create DSClient");
        }        
        Document document = null;
        //LOG.debug("Do capture run");
        try {
            // parse the input into a DOM
            document = parseInput(in, null);

            // validate incoming document against its schema
            if (isEPCISDocument(document)) {
                validateDocument(document, getSchema());
            } else if (isEPCISMasterDataDocument(document)) {
                validateDocument(document, getMasterDataSchema());
            }
        } catch (IOException e) {
            throw new InternalBusinessException("unable to read from input: " + e.getMessage(), e);
        }

        try {
            if (session == null) throw new InternalBusinessException("Could not connect to Cassandra cluster");
            if (isEPCISDocument(document)) {
                processEvents(session, document);
            } else if (isEPCISMasterDataDocument(document)) {
                 processMasterData(session, document);
            }
            LOG.info("EPCIS Capture Interface request succeeded");
        } catch (Exception e) {
            LOG.error("EPCIS Capture Interface request failed: " + e.toString());
            throw new InternalBusinessException("EPCIS Capture Interface request failed: " + e.getMessage(), e);

        }
    }

    /**
     * Validates the given document against the given schema.
     */
    private void validateDocument(Document document, Schema schema) throws SAXException, IOException {
        if (schema != null) {
            Validator validator = schema.newValidator();
            validator.validate(new DOMSource(document));
            LOG.info("Incoming capture request was successfully validated against the EPCISDocument schema");
        } else {
            LOG.warn("Schema validator unavailable. Unable to validate EPCIS capture event against schema!");
        }
    }

    /**
     * Parses the input into a DOM. If a schema is given, the input is also
     * validated against this schema. The schema may be null.
     */
    private Document parseInput(InputStream in, Schema schema) throws InternalBusinessException, SAXException,
            IOException {
        DocumentBuilderFactory factory = DocumentBuilderFactory.newInstance();
        factory.setNamespaceAware(true);
        factory.setIgnoringComments(true);
        factory.setIgnoringElementContentWhitespace(true);
        factory.setSchema(schema);
        try {
            DocumentBuilder builder = factory.newDocumentBuilder();
            builder.setErrorHandler(new ErrorHandler() {
                public void warning(SAXParseException e) throws SAXException {
                    LOG.warn("warning while parsing XML input: " + e.getMessage());
                }

                public void fatalError(SAXParseException e) throws SAXException {
                    LOG.error("non-recovarable error while parsing XML input: " + e.getMessage());
                    throw e;
                }

                public void error(SAXParseException e) throws SAXException {
                    LOG.error("error while parsing XML input: " + e.getMessage());
                    throw e;
                }
            });
            Document document = builder.parse(in);
            LOG.debug("payload successfully parsed as XML document");
            if (LOG.isDebugEnabled()) {
                logDocument(document);
            }
            return document;
        } catch (ParserConfigurationException e) {
            throw new InternalBusinessException("unable to configure document builder to parse XML input", e);
        }
    }

    /**
     * prints the given Document to the log file if it does not exceed a
     * specified size.
     */
    private void logDocument(Document document) {
        try {
            TransformerFactory tfFactory = TransformerFactory.newInstance();
            Transformer transformer = tfFactory.newTransformer();
            StringWriter writer = new StringWriter();
            transformer.transform(new DOMSource(document), new StreamResult(writer));
            String xml = writer.toString();
            if (xml.length() > 100 * 1024) {
                // too large, do not log
                xml = null;
            } else {
                LOG.debug("Incoming contents:\n\n" + writer.toString() + "\n");
            }
        } catch (Throwable t) {
            // never mind ... do not log
        }
    }

    /**
     * @return <code>true</code> if the given Document is an
     *         <i>EPCISDocument</i>.
     */
    private boolean isEPCISDocument(Document document) {
        return document.getDocumentElement().getLocalName().equals("EPCISDocument");
    }

    /**
     * @return <code>true</code> if the given Document is an
     *         <i>EPCISMasterDataDocument</i>.
     */
    private boolean isEPCISMasterDataDocument(Document document) {
        return document.getDocumentElement().getLocalName().equals("EPCISMasterDataDocument");
    }

    /**
     * Processes the given document and stores the events to db.
     */
    private void processEvents(Session session, Document document) throws DOMException, SAXException,
            InvalidFormatException {
        NodeList eventList = document.getElementsByTagName("EventList");
        NodeList events = eventList.item(0).getChildNodes();

        // walk through all supplied events
        // int eventCount = 0;
        for (int i = 0; i < events.getLength(); i++) {
            Node eventNode = events.item(i);
            String nodeName = eventNode.getNodeName();

            if (nodeName.equals(EpcisConstants.OBJECT_EVENT) || nodeName.equals(EpcisConstants.AGGREGATION_EVENT)
                    || nodeName.equals(EpcisConstants.QUANTITY_EVENT)
                    || nodeName.equals(EpcisConstants.TRANSACTION_EVENT)) {
                LOG.debug("processing event " + i + ": '" + nodeName + "'.");
                handleEvent(session, eventNode, nodeName);
            } else if (!nodeName.equals("#text") && !nodeName.equals("#comment")) {
                throw new SAXException("Encountered unknown event '" + nodeName + "'.");
            }
        }
    }

    /**
     * Takes an XML document node, parses it as EPCIS event and inserts the data
     * into the database. The parse routine is generic for all event types; the
     * query generation part has some if/elses to take care of different event
     * parameters.
     * 
     * @param eventNode
     *            The current event node.
     * @param eventType
     *            The current event type.
     * @throws Exception
     * @throws DOMException
     */
    private void handleEvent(final Session session, final Node eventNode, final String eventType) throws DOMException,
            SAXException, InvalidFormatException {
        if (eventNode == null) {
            // nothing to do
            return;
        } else if (eventNode.getChildNodes().getLength() == 0) {
            throw new SAXException("Event element '" + eventNode.getNodeName() + "' has no children elements.");
        }
        Node curEventNode = null;
        //LOG.debug("Module -- handEvent run");

        // A lot of the initialized variables have type URI. This type isn't to
        // compare with the URI-Type of the standard. In fact, most of the
        // variables having type URI are declared as Vocabularies in the
        // Standard. Commonly, we use String for the Standard-Type URI.

        Calendar eventTime = null;
        Calendar recordTime = GregorianCalendar.getInstance();
        recordTime.set(Calendar.MILLISECOND, 0); 
        
        String eventTimeZoneOffset = null;
        String action = null;
        String parentId = null;
        Long quantity = null;
        String bizStepUri = null;
        String dispositionUri = null;
        String readPointUri = null;
        String bizLocationUri = null;
        String epcClassUri = null;

        List<String> epcs = null;        
        Set<String> epcset = null;       
        Map<String, String> bizTransMap = null;
        List<EventFieldExtension> fieldNameExtList = new ArrayList<EventFieldExtension>();        
        Date date = null;
        String rowkey_addition = null;
        
        for (int i = 0; i < eventNode.getChildNodes().getLength(); i++) {
            curEventNode = eventNode.getChildNodes().item(i);
            String nodeName = curEventNode.getNodeName();

            if (nodeName.equals("#text") || nodeName.equals("#comment")) {
                // ignore text or comments
                LOG.debug("  ignoring text or comment: '" + curEventNode.getTextContent().trim() + "'");
                continue;
            }

            LOG.debug("  handling event field: '" + nodeName + "'");
            if (nodeName.equals("eventTime")) {
                String xmlTime = curEventNode.getTextContent();
                LOG.debug("    eventTime in xml is '" + xmlTime + "'");
                try {
                    eventTime = TimeParser.parseAsCalendar(xmlTime);
                    date = eventTime.getTime();
                    rowkey_addition = "|" + String.valueOf(eventTime.get(Calendar.YEAR))
                            + String.valueOf(eventTime.get(Calendar.MONTH) + 1);
                } catch (ParseException e) {
                    throw new SAXException("Invalid date/time (must be ISO8601).", e);
                }
                LOG.debug("    eventTime parsed as '" + eventTime.getTime() + "'");
            } else if (nodeName.equals("recordTime")) {
                // ignore recordTime
            } else if (nodeName.equals("eventTimeZoneOffset")) {
                eventTimeZoneOffset = checkEventTimeZoneOffset(curEventNode.getTextContent());
            } else if (nodeName.equals("epcList") || nodeName.equals("childEPCs")) {
                epcs = handleEpcs(eventType, curEventNode);
            } else if (nodeName.equals("bizTransactionList")) {
                bizTransMap = handleBizTransactions(curEventNode);
                
            } else if (nodeName.equals("action")) {
                action = curEventNode.getTextContent();
                if (!action.equals("ADD") && !action.equals("OBSERVE") && !action.equals("DELETE")) {
                    throw new SAXException("Encountered illegal 'action' value: " + action);
                }
            } else if (nodeName.equals("bizStep")) {
                bizStepUri = curEventNode.getTextContent();
            } else if (nodeName.equals("disposition")) {
                dispositionUri = curEventNode.getTextContent();
            } else if (nodeName.equals("readPoint")) {
                Element attrElem = (Element) curEventNode;
                Node id = attrElem.getElementsByTagName("id").item(0);
                readPointUri = id.getTextContent();
            } else if (nodeName.equals("bizLocation")) {
                Element attrElem = (Element) curEventNode;
                Node id = attrElem.getElementsByTagName("id").item(0);
                bizLocationUri = id.getTextContent();
            } else if (nodeName.equals("epcClass")) {
                epcClassUri = curEventNode.getTextContent();
            } else if (nodeName.equals("quantity")) {
                quantity = Long.valueOf(curEventNode.getTextContent());
            } else if (nodeName.equals("parentID")) {
                checkEpcOrUri(curEventNode.getTextContent(), true);
                parentId = curEventNode.getTextContent();
                LOG.debug("parent ID checked!!"+ parentId);
            } else {
                String[] parts = nodeName.split(":");
                LOG.debug("Nodename: " + nodeName);
                
                LOG.debug(parts[0]);
                LOG.debug(parts[1]);

                if (parts.length == 2) {
                    LOG.debug("    treating unknown event field as extension.");
                    String prefix = parts[0];
                    String localname = parts[1];
                    String namespace = curEventNode.lookupNamespaceURI(prefix);
                    String value = curEventNode.getTextContent();
                    EventFieldExtension evf = new EventFieldExtension(prefix, namespace, localname, value);
                    fieldNameExtList.add(evf);
                } else {
                    // this is not a valid extension
                    throw new SAXException("    encountered unknown event field: '" + nodeName + "'.");
                }
            }
        }
        if (eventType.equals(EpcisConstants.AGGREGATION_EVENT)) {
            // for AggregationEvents, the parentID is only optional for
            // action=OBSERVE
            if (parentId == null && ("ADD".equals(action) || "DELETE".equals(action))) {
                
                throw new InvalidFormatException("'parentID' is required if 'action' is ADD or DELETE");
            }
        }

        String nodeName = eventNode.getNodeName();
        VocabularyElement bizStep = new BusinessStepId();
        bizStep.setUri(bizStepUri);
        VocabularyElement disposition = new DispositionId();
        disposition.setUri(dispositionUri);
        VocabularyElement readPoint = new ReadPointId();
        readPoint.setUri(readPointUri);
        VocabularyElement bizLocation = new BusinessLocationId();
        bizLocation.setUri(bizLocationUri);
        VocabularyElement epcClass = new EPCClass();
        epcClass.setUri(epcClassUri);

        if (nodeName.equals(EpcisConstants.AGGREGATION_EVENT)) {
            if (epcs != null && epcs.size() > 0) {
                epcset = new HashSet<String>(epcs);
                final List<String> epcList_T = epcs;
                final String rowkey_addition_T = rowkey_addition;
                final Date date_T = date;
                final String parentId_T = parentId; 
                backend.insertAggregationEvent(session, date, bizStepUri, dispositionUri,
                            readPointUri, action, parentId + rowkey_addition, bizLocationUri, bizTransMap, eventTimeZoneOffset, epcset, recordTime.getTime(), fieldNameExtList);
                final Timer timer = new Timer();
                TimerTask task = new TimerTask() {
                	@Override
                    public void run() {
                        for (int i = 0; i < epcList_T.size(); i++) {
                            backend.insertAggregationChildepc(session, epcList_T.get(i) + rowkey_addition_T, date_T, parentId_T);
                        }
                	}
                };
                timer.schedule(task, 0);
            }

        } else if (nodeName.equals(EpcisConstants.OBJECT_EVENT)) {
            if (epcs != null && epcs.size() > 0) {
                epcset = new HashSet<String>(epcs);
                backend.insertObjectEvent(session, epcs.get(0) + rowkey_addition, date, bizStepUri, dispositionUri,
                            readPointUri, action, bizLocationUri, bizTransMap, eventTimeZoneOffset, epcset, recordTime.getTime(), fieldNameExtList);
                final List<String> epcList_T = epcs;
                final String rowkey_addition_T = rowkey_addition;
                final Date date_T = date;
                final String bizStepUri_T = bizStepUri;
                final String readPointUri_T = readPointUri;
                final String dispositionUri_T = dispositionUri;
                final String action_T = action;
                final String bizLocationUri_T = bizLocationUri;
                final Map<String, String> bizTransMap_T = bizTransMap;
                final String eventTimeZoneOffset_T = eventTimeZoneOffset;
                final Set<String> epcset_T = epcset;
                final Date recordTime_T = recordTime.getTime();
                final List<EventFieldExtension> fieldNameExtList_T = fieldNameExtList;
                
                final Timer timer = new Timer();
                TimerTask task = new TimerTask() {
                    @Override
                    public void run() {
                            for (int i = 1; i < epcList_T.size(); i++) {
                                backend.insertObjectEvent(session, epcList_T.get(i) + rowkey_addition_T, date_T, bizStepUri_T, dispositionUri_T,
                                        readPointUri_T, action_T, bizLocationUri_T, bizTransMap_T, eventTimeZoneOffset_T, epcset_T, recordTime_T, fieldNameExtList_T);
                            }
                    }
                };
                timer.schedule(task, 0);

                // Run Geo-Discovery Service              
                if (isGeoDiscoveryEnable()) {                    
                    updateDiscoveryService(session, bizLocationUri, epcset);
                }
            }
        } else if (nodeName.equals(EpcisConstants.QUANTITY_EVENT)) {
            if (quantity != null) {
                backend.insertQuantityEvent(session, epcClassUri, date, bizStepUri, dispositionUri, readPointUri, quantity.intValue(), bizLocationUri, bizTransMap, eventTimeZoneOffset, recordTime.getTime(), fieldNameExtList);
            }
        } else if (nodeName.equals(EpcisConstants.TRANSACTION_EVENT)) {
            if (epcs != null && epcs.size() > 0) {
                epcset = new HashSet<String>(epcs);
                for (int i = 0; i < epcs.size(); i++)
                    backend.insertTransactionEvent(session, epcs.get(i) + rowkey_addition, date, bizStepUri, dispositionUri,
                            readPointUri, bizLocationUri, bizTransMap, eventTimeZoneOffset, epcset, recordTime.getTime(), fieldNameExtList);
            }
        } else {
            throw new SAXException("Encountered unknown event element '" + nodeName + "'.");
        }
    }

    /**
     * Processes the given document and stores the masterdata to db.
     */
     private void processMasterData(Session session, Document document) throws
     DOMException, SAXException,
     InvalidFormatException {
    
     // Handle Vocabulary List
     NodeList vocabularyList =
     document.getElementsByTagName("VocabularyList");
     if (vocabularyList.item(0).hasChildNodes()) {
     NodeList vocabularys = vocabularyList.item(0).getChildNodes();
    
     // walk through all supplied vocabularies
     //int vocabularyCount = 0;
     for (int i = 0; i < vocabularys.getLength(); i++) {
     Node vocabularyNode = vocabularys.item(i);
     String nodeName = vocabularyNode.getNodeName();
     //LOG.debug("time: " + i + " nodename: " + nodeName.toString() );
     if (nodeName.equals("Vocabulary")) {    
     String vocabularyType =
     vocabularyNode.getAttributes().getNamedItem("type").getNodeValue();
     //LOG.debug("time: " + i + " type: " + vocabularyType );
     if (!EpcisConstants.VOCABULARY_TYPES.contains(vocabularyType)) {
         throw new SAXException("Vocabulary Type is not supported: " + vocabularyType);
     } else {
             LOG.debug("processing " + i + ": '" + nodeName + "':" + vocabularyType + ".");
             handleVocabulary(session, vocabularyNode, vocabularyType);
         }

     } else if (!nodeName.equals("#text") && !nodeName.equals("#comment")) {
     throw new SAXException("Encountered unknown vocabulary '" + nodeName +
     "'.");
     }
     }
     }
    
     }

    /**
     * (nkef) Takes an XML document node, parses it as EPCIS Master Data and
     * inserts the data into the database. The parse routine is generic for all
     * Vocabulary types;
     * 
     * @param vocNode
     *            The current vocabulary node.
     * @param vocType
     *            The current vocabulary type.
     * @throws Exception
     * @throws DOMException
     * @throws ParseException 
     */
     private void handleVocabulary(Session session, final Node vocNode, final
     String vocType) throws DOMException,
     SAXException, InvalidFormatException {
     Map<String, String> curVocPairs = new HashMap<String, String>();
     if (vocNode == null) {
     // nothing to do
     return;
     } else if (vocNode.getChildNodes().getLength() == 0) {
     throw new SAXException("Vocabulary element '" + vocNode.getNodeName() +
     "' has no children elements.");
     }
    
     for (int i = 0; i < vocNode.getChildNodes().getLength(); i++) {
     Node curVocNode = vocNode.getChildNodes().item(i);
     if (isTextOrComment(curVocNode)) {
     continue;
     }
     for (int j = 0; j < curVocNode.getChildNodes().getLength(); j++) {
     Node curVocElemNode = curVocNode.getChildNodes().item(j);
     if (isTextOrComment(curVocElemNode)) {
     continue;
     }
     //LOG.debug("  processing vocabulary '" + curVocElemNode.getNodeName() + "'");
     LOG.debug("  processing vocabulary '" + curVocElemNode.getNodeName() +
     "'");
     String curVocElemId =
     curVocElemNode.getAttributes().getNamedItem("id").getNodeValue();
     LOG.debug("Get node name: " + vocNode.getNodeName());
     LOG.debug("curVocElemId: " + curVocElemId);
     
     /*
     * vocabularyElementEditMode 1: insert((it can be anything
     * except 2,3,4)) 2: alterURI 3: singleDelete 4: Delete element
     * with it's direct or indirect descendants
     */
     String vocElemEditMode = "";
    
     if (!(curVocElemNode.getAttributes().getNamedItem("mode") == null)) {
     vocElemEditMode =
     curVocElemNode.getAttributes().getNamedItem("mode").getNodeValue();
     } else {
     vocElemEditMode = "1";
     }
     
     if ((!vocElemEditMode.equals("1")) && (!vocElemEditMode.equals("3")) && (!vocElemEditMode.equals("2"))) {
         throw new InvalidFormatException("Masterdata mode should be 1 or 2 or 3");
     }
     
     
     LOG.debug("Delete?: " + vocElemEditMode);
    
//     VocabularyElement curVocElem = getOrEditVocabularyElement(session,
//     vocType, curVocElemId,
//     vocElemEditMode);
    
     // *****************************************
     if (vocElemEditMode.equals("3")) {
         backend.deleteMasterdata(session, curVocElemId);
         LOG.debug("Delete masterdata");
     } else {         
         for (int k = 0; k < curVocElemNode.getChildNodes().getLength(); k++) {
         Node curVocAttrNode = curVocElemNode.getChildNodes().item(k);
         if (isTextOrComment(curVocAttrNode)) {
         continue;
         }
        
         LOG.debug("  processing vocabulary attribute '" + curVocAttrNode.getNodeName() + "'");
         String curVocAttrId =
         curVocAttrNode.getAttributes().getNamedItem("id").getNodeValue().replace(":", "_");
         String curVocAttrValue = parseVocAttributeValue(curVocAttrNode);
         curVocPairs.put(curVocAttrId, curVocAttrValue);
         }
         
         backend.insertMasterdata(session, curVocElemId, curVocPairs, vocabularyTablenameMap.get(vocType), vocElemEditMode);
         }
         // *****************************************
     }
         
     }

     }



    private boolean isTextOrComment(Node node) {
        return node.getNodeType() == Node.TEXT_NODE || node.getNodeType() == Node.COMMENT_NODE;
    }

    public CaptureOperationsBackendCassandra getBackend() {
        return backend;
    }

    public void setBackend(CaptureOperationsBackendCassandra backend) {
        this.backend = backend;
    }

    /**
     * Parses the attribute <b>value</b> of a VocabularyElement. The value can
     * be null in which case an empty String is returned. Otherwise, the value
     * is either given as XML attribute named 'value' or as inline text, see the
     * sample below. <br>
     * 
     * <pre>
     * {@code
     * <VocabularyElement id="urn:epc:id:sgln:0037000.00729.0">
     *   <attribute id="urn:epcglobal:fmcg:mda:slt:retail"/>
     *   <attribute id="urn:epcglobal:fmcg:mda:location">Warehouse 1</attribute>
     *   <attribute id="urn:epcglobal:fmcg:mda:room" value="22b"/>
     *   <attribute id="urn:epcglobal:fmcg:mda:address">
     *     <sample:Address xmlns:sample="http://sample.com/ComplexTypeExample">
     *       <Street>100 Nowhere Street</Street>
     *       <City>Fancy</City>
     *     </sample:Address>
     *   </attribute>
     * </VocabularyElement>
     * }
     * </pre>
     * 
     * @return the attribute value as String.
     */
    private String parseVocAttributeValue(Node vocAttrNode) {
        String vocAttrValue;
        if (vocAttrNode.getAttributes().getNamedItem("value") != null) {
            // the value is given as attribute 'value'
            vocAttrValue = vocAttrNode.getAttributes().getNamedItem("value").getNodeValue();
        } else if (vocAttrNode.getChildNodes().getLength() > 1) {
            // the value is given as DOM-tree
            TransformerFactory transFactory = TransformerFactory.newInstance();
            StringWriter buffer = new StringWriter();
            try {
                Transformer transformer = transFactory.newTransformer();
                transformer.setOutputProperty(OutputKeys.OMIT_XML_DECLARATION, "yes");
                transformer.setOutputProperty(OutputKeys.INDENT, "no");
                transformer.transform(new DOMSource(vocAttrNode.getChildNodes().item(1)), new StreamResult(buffer));
                vocAttrValue = buffer.toString();
            } catch (Throwable t) {
                LOG.warn("unable to transform vocabulary attribute value (XML) into a string", t);
                vocAttrValue = vocAttrNode.getTextContent();
            }
        } else if (vocAttrNode.getFirstChild() != null) {
            // the value is given as text
            vocAttrValue = vocAttrNode.getFirstChild().getNodeValue();
        } else {
            vocAttrValue = "";
        }
        return vocAttrValue;
    }

    /**
     * Parses the xml tree for epc nodes and returns a list of EPC URIs.
     * 
     * @param eventType
     * @param epcNode
     *            The parent Node from which EPC URIs should be extracted.
     * @return An array of vocabularies containing all the URIs found in the
     *         given node.
     * @throws SAXException
     *             If an unknown tag (no &lt;epc&gt;) is encountered.
     * @throws InvalidFormatException
     * @throws DOMException
     */
    private List<String> handleEpcs(final String eventType, final Node epcNode) throws SAXException, DOMException,
            InvalidFormatException {
        List<String> epcList = new ArrayList<String>();

        boolean isEpc = false;
        boolean epcRequired = false;
        boolean atLeastOneNonEpc = false;
        for (int i = 0; i < epcNode.getChildNodes().getLength(); i++) {
            Node curNode = epcNode.getChildNodes().item(i);
            if (curNode.getNodeName().equals("epc")) {
                isEpc = checkEpcOrUri(curNode.getTextContent(), epcRequired);
                if (isEpc) {
                    // if one of the values is an EPC, then all of them must be
                    // valid EPCs
                    epcRequired = true;
                } else {
                    atLeastOneNonEpc = true;
                }
                epcList.add(curNode.getTextContent());
            } else {
                if (curNode.getNodeName() != "#text" && curNode.getNodeName() != "#comment") {
                    throw new SAXException("Unknown XML tag: " + curNode.getNodeName(), null);
                }
            }
        }
        if (atLeastOneNonEpc && isEpc) {
            throw new InvalidFormatException(
                    "One of the provided EPCs was a 'pure identity' EPC, so all of them must be 'pure identity' EPCs");
        }
        return epcList;
    }

    /**
     * @param epcOrUri
     *            The EPC or URI to check.
     * @param epcRequired
     *            <code>true</code> if an EPC is required (will throw an
     *            InvalidFormatException if the given <code>epcOrUri</code> is
     *            an invalid EPC, but might be a valid URI), <code>false</code>
     *            otherwise.
     * @return <code>true</code> if the given <code>epcOrUri</code> is a valid
     *         EPC, <code>false</code> otherwise.
     * @throws InvalidFormatException
     */
    protected boolean checkEpcOrUri(String epcOrUri, boolean epcRequired) throws InvalidFormatException {
        boolean isEpc = false;
        if (epcOrUri.startsWith("urn:epc:id:")) {
            // check if it is a valid EPC
            checkEpc(epcOrUri);
            isEpc = true;
        } else {
            // childEPCs in AggregationEvents, and epcList in
            // TransactionEvents might also be simple URIs
            if (epcRequired) {
                throw new InvalidFormatException(
                        "One of the provided EPCs was a 'pure identity' EPC, so all of them must be 'pure identity' EPCs");
            }
            checkUri(epcOrUri);
        }
        return isEpc;
    }

    /**
     * Parses the xml tree for epc nodes and returns a List of BizTransaction
     * URIs with their corresponding type.
     * 
     * @param bizNode
     *            The parent Node from which BizTransaction URIs should be
     *            extracted.
     * @return A List of BizTransaction.
     * @throws SAXException
     *             If an unknown tag (no &lt;epc&gt;) is encountered.
     */
//     private List<BusinessTransaction> handleBizTransactions(Session session,
//     Node bizNode) throws SAXException {
//     List<BusinessTransaction> bizTransactionList = new
//     ArrayList<BusinessTransaction>();
//    
//     for (int i = 0; i < bizNode.getChildNodes().getLength(); i++) {
//     Node curNode = bizNode.getChildNodes().item(i);
//     if (curNode.getNodeName().equals("bizTransaction")) {
//    
//     // Changed by nkef (use "getOrEditVocabularyElement" instead of
//     // "getOrInsertVocabularyElement")
//     String bizTransTypeUri =
//     curNode.getAttributes().item(0).getTextContent();
//     String bizTransUri = curNode.getTextContent();
//     BusinessTransactionId bizTrans = (BusinessTransactionId)
//     getOrEditVocabularyElement(session,
//     EpcisConstants.BUSINESS_TRANSACTION_ID, bizTransUri.toString(), "1");
//     BusinessTransactionTypeId type = (BusinessTransactionTypeId)
//     getOrEditVocabularyElement(session,
//     EpcisConstants.BUSINESS_TRANSACTION_TYPE_ID, bizTransTypeUri.toString(),
//     "1");
//    
//     Criteria c0 = session.createCriteria(BusinessTransaction.class);
//     c0.add(Restrictions.eq("bizTransaction", bizTrans));
//     c0.add(Restrictions.eq("type", type));
//     BusinessTransaction bizTransaction = (BusinessTransaction)
//     c0.uniqueResult();
//    
//     if (bizTransaction == null) {
//     bizTransaction = new BusinessTransaction();
//     bizTransaction.setBizTransaction(bizTrans);
//     bizTransaction.setType(type);
//     session.save(bizTransaction);
//     }
//    
//     bizTransactionList.add(bizTransaction);
//    
//     } else {
//     if (!curNode.getNodeName().equals("#text") &&
//     !curNode.getNodeName().equals("#comment")) {
//     throw new SAXException("Unknown XML tag: " + curNode.getNodeName(),
//     null);
//     }
//     }
//     }
//     return bizTransactionList;
//     }
    
    private Map<String, String> handleBizTransactions(Node bizNode) throws SAXException {
        Map<String, String> mMap = new HashMap<String, String>();
        for (int i = 0; i < bizNode.getChildNodes().getLength(); i++) {
            Node curNode = bizNode.getChildNodes().item(i);
            if (curNode.getNodeName().equals("bizTransaction")) {
                String bizTransTypeUri =
                        curNode.getAttributes().item(0).getTextContent();
                //remove blank and new line
                String bizTransUri = curNode.getTextContent().replace("\n", "").replace("\r", "").replaceAll("\\s+", "");
                mMap.put(bizTransTypeUri, bizTransUri);
            } else {
                if (!curNode.getNodeName().equals("#text") &&
                        !curNode.getNodeName().equals("#comment")) {
                    throw new SAXException("Unknown XML tag: " + curNode.getNodeName(),
                            null);
                }
            }
        }
        return mMap;
    }

    /**
     * (depricated) Inserts vocabulary into the database by searching for
     * already existing entries; if found, the corresponding ID is returned. If
     * not found, the vocabulary is extended if "insertmissingvoc" is true;
     * otherwise an SQLException is thrown
     * 
     * @param tableName
     *            The name of the vocabulary table.
     * @param uri
     *            The vocabulary adapting the URI to be inserted into the
     *            vocabulary table.
     * @return The ID of an already existing vocabulary table with the given
     *         uri.
     * @throws UnsupportedOperationException
     *             If we are not allowed to insert a missing vocabulary.
     */
    // public VocabularyElement getOrInsertVocabularyElement(Session session,
    // String vocabularyType,
    // String vocabularyElement) throws SAXException {
    // Class<?> c = vocClassMap.get(vocabularyType);
    // Criteria c0 = session.createCriteria(c);
    // c0.setCacheable(true);
    // c0.add(Restrictions.eq("uri", vocabularyElement));
    // VocabularyElement ve;
    // try {
    // ve = (VocabularyElement) c0.uniqueResult();
    // } catch (ObjectNotFoundException e) {
    // ve = null;
    // }
    // if (ve == null) {
    // // the uri does not yet exist: insert it if allowed. According to
    // // the specs, some vocabulary is not allowed to be extended; this is
    // // currently ignored here
    // if (!insertMissingVoc) {
    // throw new
    // UnsupportedOperationException("Not allowed to add new vocabulary - use existing vocabulary");
    // } else {
    // // VocabularyElement subclasses should always have public
    // // zero-arg constructor to avoid problems here
    // try {
    // ve = (VocabularyElement) c.newInstance();
    // } catch (InstantiationException e) {
    // throw new RuntimeException(e);
    // } catch (IllegalAccessException e) {
    // throw new RuntimeException(e);
    // }
    //
    // ve.setUri(vocabularyElement);
    // session.save(ve);
    // session.flush();
    // }
    // }
    // return ve;
    // }

    /**
     * (changed by nkef to support MasterDataCapture. Previusly known as
     * "getOrInsertVocabularyElement") Inserts vocabulary into the database by
     * searching for already existing entries; if found, the corresponding ID is
     * returned. If not found, the vocabulary is extended if "insertmissingvoc"
     * is true; otherwise an SQLException is thrown
     * 
     * @param tableName
     *            The name of the vocabulary table.
     * @param uri
     *            The vocabulary adapting the URI to be inserted into the
     *            vocabulary table.
     * @return The ID of an already existing vocabulary table with the given
     *         uri.
     * @throws UnsupportedOperationException
     *             If we are not allowed to insert a missing vocabulary.
     */
    // public VocabularyElement getOrEditVocabularyElement(Session session,
    // String vocabularyType,
    // String vocabularyElementURI, String mode) throws SAXException {
    // boolean alterURI = false;
    // boolean singleDelete = false;
    // boolean wdDelete = false;
    // Long vocabularyElementID = null;
    //
    // if (mode.equals("2")) {
    // alterURI = true;
    // } else if (mode.equals("3")) {
    // singleDelete = true;
    // } else if (mode.equals("4")) {
    // wdDelete = true;
    // }
    //
    // Class<?> c = vocClassMap.get(vocabularyType);
    // Criteria c0 = session.createCriteria(c);
    // c0.setCacheable(true);
    // c0.add(Restrictions.eq("uri", alterURI ?
    // vocabularyElementURI.split("#")[0] : vocabularyElementURI));
    // VocabularyElement ve;
    // try {
    // ve = (VocabularyElement) c0.uniqueResult();
    // } catch (ObjectNotFoundException e) {
    // ve = null;
    // }
    // if (ve != null) {
    // vocabularyElementID = ve.getId();
    // }
    //
    // if (ve == null || ((singleDelete || alterURI || wdDelete) && ve != null))
    // {
    // // the uri does not yet exist: insert it if allowed. According to
    // // the specs, some vocabulary is not allowed to be extended; this is
    // // currently ignored here
    // if (!insertMissingVoc) {
    // throw new
    // UnsupportedOperationException("Not allowed to add new vocabulary - use existing vocabulary");
    // } else {
    // // VocabularyElement subclasses should always have public
    // // zero-arg constructor to avoid problems here
    //
    // if (alterURI) {
    // ve.setUri(vocabularyElementURI.split("#")[1]);
    // session.update(ve);
    // session.flush();
    // return ve;
    //
    // } else if (singleDelete) {
    // Object vocabularyElementObject = session.get(c, vocabularyElementID);
    // if (vocabularyElementObject != null)
    // session.delete(vocabularyElementObject);
    // deleteVocabularyElementAttributes(session, vocabularyType,
    // vocabularyElementID);
    // session.flush();
    // return null;
    // } else if (wdDelete) {
    // Object vocabularyElementObject = session.get(c, vocabularyElementID);
    // if (vocabularyElementObject != null)
    // session.delete(vocabularyElementObject);
    // deleteVocabularyElementAttributes(session, vocabularyType,
    // vocabularyElementID);
    // deleteVocabularyElementDescendants(session, vocabularyType,
    // vocabularyElementURI);
    // session.flush();
    // return null;
    //
    // } else {
    //
    // try {
    // ve = (VocabularyElement) c.newInstance();
    // } catch (InstantiationException e) {
    // throw new RuntimeException(e);
    // } catch (IllegalAccessException e) {
    // throw new RuntimeException(e);
    // }
    //
    // ve.setUri(vocabularyElementURI);
    // session.save(ve);
    // }
    //
    // session.flush();
    // }
    // }
    // return ve;
    // }

    /**
     * (nkef) Delete the a vocabulary's Element Descendants and all of their
     * Attributes
     * 
     * @param session
     * @param vocabularyType
     * @param vocabularyElementURI
     */
    // private void deleteVocabularyElementDescendants(Session session, String
    // vocabularyType, String vocabularyElementURI) {
    // Class<?> c = vocClassMap.get(vocabularyType);
    // List<?> vocElementChildrens = session.createSQLQuery(
    // "SELECT * FROM " + vocabularyTablenameMap.get(vocabularyType) +
    // " WHERE uri LIKE '"
    // + vocabularyElementURI + ",%'").addEntity(c).list();
    // for (int i = 0; i < vocElementChildrens.size(); i++) {
    // session.delete((VocabularyElement) vocElementChildrens.get(i));
    // deleteVocabularyElementAttributes(session, vocabularyType,
    // ((VocabularyElement) vocElementChildrens.get(i)).getId());
    // }
    // session.flush();
    // }

    /**
     * (nkef) Delete selected id vocabulary elements attributes
     * 
     * @param session
     * @param vocabularyType
     * @param vocabularyElementID
     */
    // private void deleteVocabularyElementAttributes(Session session, String
    // vocabularyType, Long vocabularyElementID) {
    // Class<?> c = vocAttributeClassMap.get(vocabularyType);
    // List<?> vocAttributeElements = session.createSQLQuery(
    // "select * FROM " + vocAttributeTablesMap.get(vocabularyType) +
    // " where id = '" + vocabularyElementID
    // + "'").addEntity(c).list();
    // for (int i = 0; i < vocAttributeElements.size(); i++) {
    // session.delete((VocabularyAttributeElement) vocAttributeElements.get(i));
    // }
    // session.flush();
    //
    // }

    /**
     * (nkef) Inserts vocabulary attribute into the database by searching for
     * already existing entries; if found, the corresponding ID is returned. If
     * not found, the vocabulary is extended if "insertmissingvoc" is true;
     * otherwise an SQLException is thrown
     * 
     * @param tableName
     *            The name of the vocabulary table.
     * @param uri
     *            The vocabulary adapting the URI to be inserted into the
     *            vocabulary table.
     * @return The ID of an already existing vocabulary table with the given
     *         uri.
     * @throws UnsupportedOperationException
     *             If we are not allowed to insert a missing vocabulary.
     */
    // public VocabularyAttributeElement
    // getOrEditVocabularyAttributeElement(Session session, String
    // vocabularyType,
    // Long vocabularyElementID, String vocabularyAttributeElement, String
    // vocabularyAttributeElementValue,
    // String mode) throws SAXException {
    //
    // boolean deleteAttribute = false;
    //
    // if (mode.equals("3")) {
    // deleteAttribute = true;
    // }
    // Class<?> c = vocAttributeClassMap.get(vocabularyType);
    // Criteria c0 = session.createCriteria(c);
    // c0.setCacheable(true);
    //
    // VocabularyAttrCiD vocabularyAttrCiD = new VocabularyAttrCiD();
    // vocabularyAttrCiD.setAttribute(vocabularyAttributeElement);
    // vocabularyAttrCiD.setId(vocabularyElementID);
    //
    // c0.add(Restrictions.idEq(vocabularyAttrCiD));
    //
    // VocabularyAttributeElement vocAttributeElement = null;
    //
    // try {
    // vocAttributeElement = (VocabularyAttributeElement) c0.uniqueResult();
    // } catch (ObjectNotFoundException e) {
    // vocAttributeElement = null;
    // e.printStackTrace();
    // }
    //
    // if (vocAttributeElement == null || (deleteAttribute &&
    // (vocAttributeElement != null))
    // || vocAttributeElement != null) {
    // // the uri does not yet exist: insert it if allowed. According to
    // // the specs, some vocabulary is not allowed to be extended; this is
    // // currently ignored here
    // if (!insertMissingVoc) {
    // throw new
    // UnsupportedOperationException("Not allowed to add new vocabulary - use existing vocabulary");
    // } else {
    // // VocabularyAttributeElement subclasses should always have
    // // public zero-arg constructor to avoid problems here
    // try {
    // vocAttributeElement = (VocabularyAttributeElement) c.newInstance();
    // } catch (InstantiationException e) {
    // throw new RuntimeException(e);
    // } catch (IllegalAccessException e) {
    // throw new RuntimeException(e);
    // }
    // vocAttributeElement.setVocabularyAttrCiD(vocabularyAttrCiD);
    // vocAttributeElement.setValue(vocabularyAttributeElementValue);
    //
    // if (vocAttributeElement == null) {
    // session.save(vocAttributeElement);
    // }
    //
    // else if (deleteAttribute) {
    // Object vocabularyAttr = session.get(c, vocabularyAttrCiD);
    // if (vocabularyAttr != null) session.delete(vocabularyAttr);
    // session.flush();
    // return null;
    // } else {
    // session.merge(vocAttributeElement);
    // }
    //
    // session.flush();
    // }
    // }
    //
    // return vocAttributeElement;
    // }

    /**
     * TODO: javadoc!
     * 
     * @param textContent
     * @return
     * @throws InvalidFormatException
     */
    protected String checkEventTimeZoneOffset(String textContent) throws InvalidFormatException {
        // first check the provided String against the expected pattern
        Pattern p = Pattern.compile("[+-]\\d\\d:\\d\\d");
        Matcher m = p.matcher(textContent);
        boolean matches = m.matches();
        if (matches) {
            // second check the values (hours and minutes)
            int h = Integer.parseInt(textContent.substring(1, 3));
            int min = Integer.parseInt(textContent.substring(4, 6));
            if ((h < 0) || (h > 14)) {
                matches = false;
            } else if (h == 14 && min != 0) {
                matches = false;
            } else if ((min < 0) || (min > 59)) {
                matches = false;
            }
        }
        if (matches) {
            return textContent;
        } else {
            throw new InvalidFormatException("'eventTimeZoneOffset' has invalid format: " + textContent);
        }
    }

    /**
     * TODO: javadoc!
     * 
     * @param textContent
     * @return
     * @throws InvalidFormatException
     */
    private boolean checkUri(String textContent) throws InvalidFormatException {
        try {
            new URI(textContent);
        } catch (URISyntaxException e) {
            throw new InvalidFormatException(e.getMessage(), e);
        }
        return true;
    }

    /**
     * Check EPC according to 'pure identity' URI as specified in Tag Data
     * Standard.
     * 
     * @param textContent
     * @throws InvalidFormatException
     */
    protected void checkEpc(String textContent) throws InvalidFormatException {
        String uri = textContent;
        if (!uri.startsWith("urn:epc:id:")) {
            throw new InvalidFormatException("Invalid 'pure identity' EPC format: must start with \"urn:epc:id:\"");
        }
        uri = uri.substring("urn:epc:id:".length());

        // check the patterns for the different EPC types
        String epcType = uri.substring(0, uri.indexOf(":"));
        uri = uri.substring(epcType.length() + 1);
        LOG.debug("Checking pattern for EPC type " + epcType + ": " + uri);
        Pattern p;
        if ("gid".equals(epcType)) {
            p = Pattern.compile("((0|[1-9][0-9]*)\\.){2}(0|[1-9][0-9]*)");
        } else if ("sgtin".equals(epcType) || "sgln".equals(epcType) || "grai".equals(epcType)) {
            p = Pattern.compile("([0-9]+\\.){2}([0-9]|[A-Z]|[a-z]|[\\!\\(\\)\\*\\+\\-',:;=_]|(%(([0-9]|[A-F])|[a-f]){2}))+");
        } else if ("sscc".equals(epcType)) {
            p = Pattern.compile("[0-9]+\\.[0-9]+");
        } else if ("giai".equals(epcType)) {
            p = Pattern.compile("[0-9]+\\.([0-9]|[A-Z]|[a-z]|[\\!\\(\\)\\*\\+\\-',:;=_]|(%(([0-9]|[A-F])|[a-f]){2}))+");
        } else {
            throw new InvalidFormatException("Invalid 'pure identity' EPC format: unknown EPC type: " + epcType);
        }
        Matcher m = p.matcher(uri);
        if (!m.matches()) {
            throw new InvalidFormatException("Invalid 'pure identity' EPC format: pattern \"" + uri
                    + "\" is invalid for EPC type \"" + epcType + "\" - check with Tag Data Standard");
        }

        // check the number of digits for the different EPC types
        boolean exceeded = false;
        int count1 = uri.indexOf(".");
        if ("sgtin".equals(epcType)) {
            int count2 = uri.indexOf(".", count1 + 1) - (count1 + 1);
            if (count1 + count2 > 13) {
                exceeded = true;
            }
        } else if ("sgln".equals(epcType)) {
            int count2 = uri.indexOf(".", count1 + 1) - (count1 + 1);
            if (count1 + count2 > 12) {
                exceeded = true;
            }
        } else if ("grai".equals(epcType)) {
            int count2 = uri.indexOf(".", count1 + 1) - (count1 + 1);
            if (count1 + count2 > 12) {
                exceeded = true;
            }
        } else if ("sscc".equals(epcType)) {
            int count2 = uri.length() - (count1 + 1);
            if (count1 + count2 > 17) {
                exceeded = true;
            }
        } else if ("giai".equals(epcType)) {
            int count2 = uri.length() - (count1 + 1);
            if (count1 + count2 > 30) {
                exceeded = true;
            }
        } else {
            // nothing to count
        }
        if (exceeded) {
            throw new InvalidFormatException(
                    "Invalid 'pure identity' EPC format: check allowed number of characters for EPC type '" + epcType
                            + "'");
        }
    }

//    public SessionFactory getSessionFactory() {
//        return sessionFactory;
//    }
//
//    public void setSessionFactory(SessionFactory sessionFactory) {
//        this.sessionFactory = sessionFactory;
//    }
    
    private void updateDiscoveryService(Session session, final String bizlocation, final Set<String> epcset) {
        try {
            long startTime = System.currentTimeMillis();
            //Geo Location Update part
            //TODO: EPCIS URL need to be dynamic
            if (bizlocation == null) return;
            List <String> geoLocation = backend.getGeoLocation(session, bizlocation);
            //ESclient esClient = new ESclient(geoDiscoveryURL, geoDiscoveryPORT);
            if (geoLocation != null) {                                
                double longtitude = Double.parseDouble(geoLocation.get(0));
                double latitude = Double.parseDouble(geoLocation.get(1));               
                for (String geoEpc:epcset) {
                    esClient.Send_Registration(geoEpc,
                            epcisRepositoryURL,
                            new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()), longtitude, latitude);
                    LOG.debug("Update EPC to Discovery Service: ");
                    LOG.debug("EPC: " + geoEpc);
                    LOG.debug("geoDiscoveryURL: " + geoDiscoveryURL);
                    LOG.debug("geoDiscoveryPORT: " + geoDiscoveryPORT);
                    LOG.debug("EPCIS Repository: " + epcisRepositoryURL);
                }                
            } else {
                for (String geoEpc:epcset) {
                    esClient.Send_Registration(geoEpc,
                            epcisRepositoryURL,
                            new SimpleDateFormat("yyyyMMddHHmmss").format(new Date()), 0, 0);
                    LOG.debug("No Geo Info. EPCIS Repository: " + epcisRepositoryURL);
            }
            }
         }  catch (NoNodeAvailableException e) {
            LOG.debug("Can not connect to ElasticSearch");
            LOG.debug("Can not update Discovery SERVICE");
        }
    }

    public boolean isDbResetAllowed() {
        return dbResetAllowed;
    }

    public void setDbResetAllowed(boolean dbResetAllowed) {
        this.dbResetAllowed = dbResetAllowed;
    }

    public boolean isInsertMissingVoc() {
        return insertMissingVoc;
    }

    public void setInsertMissingVoc(boolean insertMissingVoc) {
        this.insertMissingVoc = insertMissingVoc;
    }

    public Schema getSchema() {
        return schema;
    }

    public void setSchema(Schema schema) {
        this.schema = schema;
    }

    public void setEpcisSchemaFile(String epcisSchemaFile) {
        Schema schema = initEpcisSchema(epcisSchemaFile);
        setSchema(schema);
    }

    public void setEpcisMasterdataSchemaFile(String epcisMasterdataSchemaFile) {
        Schema schema = initEpcisSchema(epcisMasterdataSchemaFile);
        setMasterDataSchema(schema);
    }

    public Schema getMasterDataSchema() {
        return masterDataSchema;
    }

    public void setMasterDataSchema(Schema masterDataSchema) {
        this.masterDataSchema = masterDataSchema;
    }
}
