package org.fosstrak.epcis.repository.query;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.Deque;
import java.util.GregorianCalendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TimeZone;




import javax.xml.bind.JAXBElement;
import javax.xml.datatype.DatatypeConfigurationException;
import javax.xml.datatype.DatatypeFactory;
import javax.xml.datatype.XMLGregorianCalendar;
import javax.xml.namespace.QName;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.fosstrak.epcis.model.ActionType;
import org.fosstrak.epcis.model.AggregationEventType;
import org.fosstrak.epcis.model.AttributeType;
import org.fosstrak.epcis.model.BusinessLocationType;
import org.fosstrak.epcis.model.BusinessTransactionListType;
import org.fosstrak.epcis.model.BusinessTransactionType;
import org.fosstrak.epcis.model.EPC;
import org.fosstrak.epcis.model.EPCISEventType;
import org.fosstrak.epcis.model.EPCListType;
import org.fosstrak.epcis.model.ImplementationException;
import org.fosstrak.epcis.model.ImplementationExceptionSeverity;
import org.fosstrak.epcis.model.ObjectEventType;
import org.fosstrak.epcis.model.QuantityEventType;
import org.fosstrak.epcis.model.QueryParams;
import org.fosstrak.epcis.model.ReadPointType;
import org.fosstrak.epcis.model.SubscriptionControls;
import org.fosstrak.epcis.model.TransactionEventType;
import org.fosstrak.epcis.model.VocabularyElementListType;
import org.fosstrak.epcis.model.VocabularyElementType;
import org.fosstrak.epcis.model.VocabularyType;
import org.fosstrak.epcis.repository.EpcisConstants;
import org.fosstrak.epcis.repository.query.SimpleEventQueryDTO.EventQueryParam;
import org.fosstrak.epcis.repository.query.SimpleEventQueryDTO.Operation;
import org.fosstrak.epcis.soap.ImplementationExceptionResponse;

import com.datastax.driver.core.BoundStatement;
import com.google.common.collect.LinkedListMultimap;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.exceptions.DriverException;
import com.datastax.driver.core.exceptions.InvalidTypeException;

/**
 * QueryOperationsBackendCassandra uses Cassandra Query Language (CQL) to perform event query towards the database. 
 * @author TuanLe
 *
 */
public class QueryOperationsBackendCassandra {
    
    private static final Log LOG = LogFactory.getLog(QueryOperationsBackendCassandra.class);
    
    private static final String CQL_SELECT_FROM_AGGREGATIONEVENT = "SELECT * FROM AggregationEvent";
    private static final String CQL_SELECT_FROM_AGGREGATIONEVENT_EPC = "SELECT * FROM EPCIndex_AggregationEvent WHERE childepc = ?";
    private static final String CQL_SELECT_FROM_OBJECTEVENT = "SELECT * FROM ObjectEvent";
    private static final String CQL_SELECT_FROM_OBJECTEVENT_LOCATION = "SELECT * FROM LocationIndex_ObjectEvent ";
    private static final String CQL_SELECT_FROM_QUANTITYEVENT = "SELECT * FROM QuantityEvent";
    private static final String CQL_SELECT_FROM_TRANSACTIONEVENT = "SELECT DISTINCT event_TransactionEvent.id, eventTime, eventTimeMs, recordTime, recordTimeMs, eventTimeZoneOffset, readPoint.uri AS readPoint, bizLocation.uri AS bizLocation, bizStep.uri AS bizStep, disposition.uri AS disposition, action, parentID FROM event_TransactionEvent LEFT JOIN voc_BizStep AS bizStep ON event_TransactionEvent.bizStep=bizStep.id LEFT JOIN voc_Disposition AS disposition ON event_TransactionEvent.disposition=disposition.id LEFT JOIN voc_ReadPoint AS readPoint ON event_TransactionEvent.readPoint=readPoint.id LEFT JOIN voc_BizLoc AS bizLocation ON event_TransactionEvent.bizLocation=bizLocation.id";
    private static final String CQL_SELECT_FROM_TRANSACTIONEVENT_LOCATION = "";
    
    private static final String CQL_SELECT_FROM_AGGREGATIONEVENT_FROM_INDEX = "SELECT * FROM AggregationEvent WHERE epc = ? AND eventtime = ?";
    private static final String CQL_SELECT_FROM_OBJECTEVENT_FROM_INDEX = "SELECT * FROM ObjectEvent WHERE epc = ? AND eventtime = ?";
    private static final String CQL_SELECT_FROM_TRANSACTIONEVENT_FROM_INDEX = "SELECT * FROM TransactionEvent WHERE epc = ? AND eventtime = ?";
   
    private static Map<Operation, String> operationMap;
    private static Map<String, String> vocabularyTablenameMap;
    private static Map<String, String> vocabularyTablenameMap_Inversed;
    private static List<List<Integer>> listPositionArrayofStringValue;
    private Boolean allowFiltering;
    
    static {     

        operationMap = new HashMap<Operation, String>(9);
        operationMap.put(Operation.EQ, "=");
        operationMap.put(Operation.GE, ">=");
        operationMap.put(Operation.LE, "<=");
        operationMap.put(Operation.GT, ">");
        operationMap.put(Operation.LT, "<");
        operationMap.put(Operation.MATCH, "LIKE");
        operationMap.put(Operation.WD, "LIKE");
        operationMap.put(Operation.EQATTR, "=");
        operationMap.put(Operation.HASATTR, "=");
        
        vocabularyTablenameMap = new HashMap<String, String>(8);
        vocabularyTablenameMap.put(EpcisConstants.BUSINESS_STEP_ID, "voc_BizStep");
        vocabularyTablenameMap.put(EpcisConstants.BUSINESS_LOCATION_ID, "voc_BizLoc");
        vocabularyTablenameMap.put(EpcisConstants.BUSINESS_TRANSACTION_ID, "voc_BizTrans");
        vocabularyTablenameMap.put(EpcisConstants.BUSINESS_TRANSACTION_TYPE_ID, "voc_BizTransType");
        vocabularyTablenameMap.put(EpcisConstants.DISPOSITION_ID, "voc_Disposition");
        vocabularyTablenameMap.put(EpcisConstants.EPC_CLASS_ID, "voc_EPCClass");
        vocabularyTablenameMap.put(EpcisConstants.READ_POINT_ID, "voc_ReadPoint");
        vocabularyTablenameMap.put(EpcisConstants.EPC_ID, "voc_EPC");
        
        vocabularyTablenameMap_Inversed = new HashMap<String, String>(8);
        vocabularyTablenameMap_Inversed.put("voc_BizStep", EpcisConstants.BUSINESS_STEP_ID);
        vocabularyTablenameMap_Inversed.put("voc_BizLoc", EpcisConstants.BUSINESS_LOCATION_ID);
        vocabularyTablenameMap_Inversed.put("voc_BizTrans", EpcisConstants.BUSINESS_TRANSACTION_ID);
        vocabularyTablenameMap_Inversed.put("voc_BizTransType", EpcisConstants.BUSINESS_TRANSACTION_TYPE_ID);
        vocabularyTablenameMap_Inversed.put("voc_Disposition", EpcisConstants.DISPOSITION_ID);
        vocabularyTablenameMap_Inversed.put("voc_EPCClass", EpcisConstants.EPC_CLASS_ID);
        vocabularyTablenameMap_Inversed.put("voc_ReadPoint", EpcisConstants.READ_POINT_ID);
        vocabularyTablenameMap_Inversed.put("voc_EPC", EpcisConstants.EPC_ID);

    }

    
    public void prepareSimpleEventQuery (Session session, SimpleEventQueryDTO seQuery, final List<Object> eventList) throws ImplementationExceptionResponse {
        StringBuilder sqlSelectFrom = null;
        StringBuilder sqlWhereClause = new StringBuilder(" WHERE 1 ");
        
        List<String> eventFields = new ArrayList<String>();
        List<Operation> ops = new ArrayList<Operation>();
        List<Object> values = new ArrayList<Object>();
        List<Object> values_aggregationEvent = new ArrayList<Object>();

        Boolean hasGE_Eventtime = false;
        Boolean hasLT_Eventtime = false;
        List <Integer> low_EventTime = new ArrayList<Integer>();
        List <Integer> high_EventTime = new ArrayList<Integer>();
        List<?> epc_list = null;
        List<?> parentID_list = null;
        
        List<EventQueryParam> eventQueryParams = seQuery.getEventQueryParams();
        String eventType = seQuery.getEventType();
        
        for (EventQueryParam queryParam : eventQueryParams) {
            String eventField = queryParam.getEventField();
            Operation op = queryParam.getOp();
            Object value = queryParam.getValue();
            if (eventField.startsWith("extension!")) {
                String temp =  eventField.substring(10);
                String[] parts = temp.split("!");
                eventField = parts[0];
            }
            eventFields.add(eventField);
            ops.add(op);
            values.add(value);
            
            if (EpcisConstants.AGGREGATION_EVENT.equals(eventType) && seQuery.hasMatchEpc()) {
            	if ("childEPCs".equals(eventField) || "parentID".equals(eventField) || "eventTime".equals(eventField)) {
            		// do nothing
            	} else {
            		values_aggregationEvent.add(value);
            	}
            }

            LOG.debug("Event field is: " + eventField);
            LOG.debug("OP is: " + op.toString());
            LOG.debug("value is: " + value.toString());

        }
        
        long LTeventTime = 0;
        long GEeventTime = 0;
        
        for (int i=0; i<eventFields.size(); i++) {            
            if (eventFields.get(i).equals("eventTime")) {
                Operation op = ops.get(i);
                String cqlOp = operationMap.get(op);
                Object value = values.get(i);
                
                if (">=".equals(cqlOp)) {
                    hasGE_Eventtime = true;
                    GEeventTime = (Long) value;
                    low_EventTime = parseTimestamp(value.toString());
                    LOG.debug("Low time: " + low_EventTime.get(0));
                    LOG.debug("Low time: " + low_EventTime.get(1));
                }
                if ("<".equals(cqlOp)) {
                    hasLT_Eventtime = true;
                    LTeventTime = (Long) value;
                    high_EventTime = parseTimestamp(value.toString());
                    LOG.debug("High time: " + high_EventTime.get(0));
                    LOG.debug("High time: " + high_EventTime.get(1));
                }                
            }

        }
        
        List<String> middle_period = new ArrayList<String>();
        if ((hasGE_Eventtime) && (hasLT_Eventtime)) {
            middle_period = middlePeriod(low_EventTime.get(0), low_EventTime.get(1), high_EventTime.get(0), high_EventTime.get(1));            
        }
        
        if ((!hasGE_Eventtime) && (hasLT_Eventtime)) {
            middle_period = middlePeriod(2000, 1, high_EventTime.get(0), high_EventTime.get(1));            
        }
        
        if ((hasGE_Eventtime) && (!hasLT_Eventtime)) {
            long now = System.currentTimeMillis();
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(now);
            middle_period = middlePeriod(low_EventTime.get(0), low_EventTime.get(1), calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH)+1);            
        }
        
        if ((!hasGE_Eventtime) && (!hasLT_Eventtime)) {
            long now = System.currentTimeMillis();
            Calendar calendar = Calendar.getInstance();
            calendar.setTimeInMillis(now);            
            // Return results within 5 years from now
            Calendar fiveYear_ago = Calendar.getInstance();
            fiveYear_ago.setTimeInMillis(now - 157784630000L);
            middle_period = middlePeriod(fiveYear_ago.get(Calendar.YEAR), fiveYear_ago.get(Calendar.MONTH)+1, calendar.get(Calendar.YEAR), calendar.get(Calendar.MONTH)+1);
        }
        StringBuilder sql = null;
        boolean hasEPCinQuery = false;
        
        if (EpcisConstants.AGGREGATION_EVENT.equals(eventType) && seQuery.hasMatchEpc()) {
        	sqlWhereClause = new StringBuilder(" WHERE parentID = ? AND eventTime = ? ");
        }
        for (int i=0; i<eventFields.size(); i++) {
            String field = eventFields.get(i);
            LOG.debug("Fields: " + i + " : " + field);
            Operation op = ops.get(i);
            String cqlOp = operationMap.get(op);              
            if ("epcList".equals(field) || "childEPCs".equals(field) || "epcClass".equals(field)) {
                epc_list = (List<?>) values.get(i);
                hasEPCinQuery = true;
            }
            
            if ("parentID".equals(field)) {
            	parentID_list = (List<?>) values.get(i);
            }

            //check for AggregationEvent, if need to query 2 column families, epcIndex first and then AggregationEvent
            if (EpcisConstants.AGGREGATION_EVENT.equals(eventType) && seQuery.hasMatchEpc()) {
            	if ("childEPCs".equals(field) || "parentID".equals(field) || "eventTime".equals(field)) {
            		// do nothing
            	} else {
            		appendEventField(sqlWhereClause, field, cqlOp);
            	}
            } else {
            	appendEventField(sqlWhereClause, field, cqlOp);
            }
        }
        
        LOG.debug("Parentid list: " + parentID_list);
        
        List<String> epc_rowkey = null;
        List<String> parentID_rowkey = null;
        if (seQuery.hasMatchEpc()) {
        	epc_rowkey = new ArrayList<String>();
        	int count = 0;
        	if (epc_list != null) {
        		for (int j = 0; j < epc_list.size(); j++)
        			for (int k = 0; k < middle_period.size(); k++) { 
        				epc_rowkey.add(epc_list.get(j).toString() + "|" + middle_period.get(k));
        				LOG.debug(epc_rowkey.get(count));
        				count++;
        			}
        	}
        } else if (EpcisConstants.AGGREGATION_EVENT.equals(eventType)) {
        	parentID_rowkey = new ArrayList<String>();
        	int count = 0;
        	if (parentID_list != null) {
        		for (int j = 0; j < parentID_list.size(); j++)
        			for (int k = 0; k < middle_period.size(); k++) { 
        				parentID_rowkey.add(parentID_list.get(j).toString() + "|" + middle_period.get(k));
        				LOG.debug(parentID_rowkey.get(count));
        				count++;
        			}
        	}
        }
        LinkedListMultimap<String, Date> parent_eventtime = null;
        if (EpcisConstants.AGGREGATION_EVENT.equals(eventType)) {
        	if (seQuery.hasMatchEpc()) {
        		parent_eventtime = getEventFromChildEPC_AggregationEvent(session, seQuery, epc_rowkey, parentID_list, LTeventTime, GEeventTime);
        		sqlSelectFrom = new StringBuilder("SELECT * FROM AGGREGATIONEVENT");
        		
        	} else {
        		//get Event from parentid. If epc is not presented then query is from aggregationevent.
        		sqlSelectFrom = new StringBuilder(CQL_SELECT_FROM_AGGREGATIONEVENT);
        	}
        } else if (EpcisConstants.OBJECT_EVENT.equals(eventType)) {
        	sqlSelectFrom = new StringBuilder(CQL_SELECT_FROM_OBJECTEVENT);
        } else if (EpcisConstants.QUANTITY_EVENT.equals(eventType)) {
        	sqlSelectFrom = new StringBuilder(CQL_SELECT_FROM_QUANTITYEVENT);
        } else if (EpcisConstants.TRANSACTION_EVENT.equals(eventType)) {
        	sqlSelectFrom = new StringBuilder(CQL_SELECT_FROM_TRANSACTIONEVENT);
        } else {
        	String msg = "Unknown event type: " + eventType;
        	LOG.error(msg);
        	ImplementationException ie = new ImplementationException();
        	ie.setReason(msg);
        	throw new ImplementationExceptionResponse(msg, ie);
        }

        if (eventFields.size() > 0) {
            //to make sure at least one append exists
            String finalsqlWhere = sqlWhereClause.toString().replace(" 1  AND", "");
            sql = sqlSelectFrom.append(finalsqlWhere);
        } else {
            sql = sqlSelectFrom;
        }

  
//        if (seQuery.getOrderBy() != null) {
//            sql.append(" ORDER BY ").append(seQuery.getOrderBy());
//            if (seQuery.getOrderDirection() != null) {
//                sql.append(" ").append(seQuery.getOrderDirection().name());
//            }
//        }
        //Default order by eventTime, CQL query for the most recent event
        if (hasEPCinQuery && seQuery.getLimit() != -1) {
            sql.append(" LIMIT ").append(seQuery.getLimit());
        } else if (seQuery.getMaxEventCount() != -1) {
            sql.append(" LIMIT ").append(seQuery.getMaxEventCount() + 1);
        }
        if (allowFiltering) sql.append(" ALLOW FILTERING");

        //LOG.debug("WHERE CLAUSE: " + sqlWhereClause.toString());
        
        
        String sqlSelect = sql.toString();
        LOG.debug("SQL SELECT:" + sqlSelect);
        LOG.debug("cqlParams size: " + values.size());
        for (int i = 0; i < values.size(); i++) {
            LOG.debug(values.get(i).toString());
        }
       if (EpcisConstants.AGGREGATION_EVENT.equals(eventType) && seQuery.hasMatchEpc()) {
    	   runQueryfromAggregationEventCF(session, sqlSelect, seQuery, parent_eventtime, values_aggregationEvent, eventList);
       } else if (EpcisConstants.AGGREGATION_EVENT.equals(eventType)) {
    	   runQueryFromEventCF(session, seQuery, sqlSelect, parentID_rowkey, eventFields, values, eventList);
       } else {
    	   runQueryFromEventCF(session, seQuery, sqlSelect, epc_rowkey, eventFields, values, eventList);
       }
       
    }
    
    private StringBuilder appendEventField(StringBuilder sb, String eventfield, String cqlOp) {
        StringBuilder result = null;
        if ("epcList".equals(eventfield)) {
            result = sb.append(" AND ").append("epc = ?");
        } else {
            result = sb.append(" AND ").append(eventfield).append(" ").append(cqlOp).append(" ?");
        }
        return result;
    }
    
    private void runQueryfromAggregationEventCF(Session session, String sqlSelect, SimpleEventQueryDTO seQuery, LinkedListMultimap<String, Date> parent_eventtime, List<Object> cqlParams, final List<Object> eventList) throws DriverException, ImplementationExceptionResponse {
    	PreparedStatement statement = session.prepare(sqlSelect);
    	BoundStatement boundStatement = new BoundStatement(statement);
    	List<Integer> arrayPosition = new ArrayList<Integer>();
    	List<List<?>> arrayPosition_value = new ArrayList<List<?>>();

    	for (int i = 0; i < cqlParams.size(); i++) {
    		if (cqlParams.get(i) instanceof List<?>) {
    			List<?> values = (List<?>) cqlParams.get(i);
    			arrayPosition.add(i);
    			arrayPosition_value.add(values);
    		} else {
    			boundStatement.setString(i+2, cqlParams.get(i).toString());
    		}
    	}

    	LOG.debug("SIZE cqlParams AggregationEvent: " + cqlParams.size());
    	for (Map.Entry<String, Date> entry : parent_eventtime.entries()) {
    		if (arrayPosition.size() > 0) {
        		for (int i=0; i<arrayPosition.size(); i++) {
        			for (int j=0; j<arrayPosition_value.get(i).size(); j++) {
//        				LOG.debug("Array position: " + arrayPosition_value.get(i).get(j).toString());
        				boundStatement.setString(arrayPosition.get(i) + 2, arrayPosition_value.get(i).get(j).toString());
        				LOG.debug("arrayPosition " + arrayPosition.get(i) + 2);
        				LOG.debug("KEY: " + entry.getKey());
        				LOG.debug("VALUE: " + entry.getValue());
        				boundStatement.setString(0, entry.getKey());
        				boundStatement.setDate(1, entry.getValue());
        				ResultSet results = session.execute(boundStatement);
        				for (Row row : results) {
        					processResults(row, seQuery, eventList);
        				}
        			}
        		}
    		} else {
    			LOG.debug("KEY: " + entry.getKey());
    			LOG.debug("VALUE: " + entry.getValue());
    			boundStatement.setString(0, entry.getKey());
    			boundStatement.setDate(1, entry.getValue());
    			ResultSet results = session.execute(boundStatement);
    			for (Row row : results) {
    				processResults(row, seQuery, eventList);
    			}
    		}
    	}
    }
    
    private LinkedListMultimap<String, Date> getEventFromChildEPC_AggregationEvent(Session session, SimpleEventQueryDTO sequery, List<String> epc_rowkey, List<?> parentID_list, long LTeventtime, long GEeventtime) {

    	LOG.debug("-----getEventFromChildEPC_AggregationEvent--------");
    	LinkedListMultimap<String, Date> parent_eventtime = LinkedListMultimap.create();
    	StringBuilder queryCQL = new StringBuilder(CQL_SELECT_FROM_AGGREGATIONEVENT_EPC);
    	if (sequery.hasGE_Eventtime()) queryCQL.append(" AND eventtime  >= ?");        
        if (sequery.hasLT_Eventtime()) queryCQL.append(" AND eventtime < ?");
        if (sequery.hasLT_Eventtime()) queryCQL.append(" LIMIT ").append(sequery.getLimit());
        String queryEPCindex = queryCQL.toString();
    	PreparedStatement statement = session.prepare(queryEPCindex);
        BoundStatement boundStatement = new BoundStatement(statement);
        int position = 1;
        if (sequery.hasGE_Eventtime()) {
        	boundStatement.setDate(position, new Date (GEeventtime));
        	position = 2;
        }
        if (sequery.hasLT_Eventtime()) {
        	boundStatement.setDate(position, new Date (LTeventtime));
        }
        
        Boolean limitResultFound = false;

        for (int i = epc_rowkey.size() - 1; i >= 0 ; i--) {
        	LOG.debug("Limit result found: " + limitResultFound);
        	if ((i>0) && (!epc_rowkey.get(i).split("\\|")[0].equals(epc_rowkey.get(i-1).split("\\|")[0]))) {
        		limitResultFound = false;
        		continue;
        	}
        	if (!limitResultFound) {
            	LOG.debug("epc rowkey: " + epc_rowkey.get(i));
                boundStatement.setString(0, epc_rowkey.get(i));
                String yyyymm = epc_rowkey.get(i).split("\\|")[1];
                 
                ResultSet results = session.execute(boundStatement);
                for (Row row : results) {
                	//If limit is defined and results are found, set the value of limitResultFound to true to limit results execution
                	if (sequery.getLimit() != -1) limitResultFound = true;
                	LOG.debug("Result From ChildEPC_AggregationEvent");
                	LOG.debug("Parent ID: " + row.getString("parentid"));
                	LOG.debug("Eventtime: " + row.getDate("eventtime"));
                	//Check if query has both epc and parentID, then selected parentID are used for query in AggregationEvent.
                	//if parentID is not given, all parentID are used for query in AggregationEvent.
                	if (parentID_list == null || parentID_list.contains(row.getString("parentid"))) {
                		parent_eventtime.put(row.getString("parentID") + "|" + yyyymm, row.getDate("eventtime"));
                	}
                }
        	} else {
        		continue;
        	}
        	
        }
        
        LOG.debug(parent_eventtime);
        return parent_eventtime;
    }
    

    private void runQueryFromEventCF(Session session, SimpleEventQueryDTO seQuery, String sqlSelect, List<String> rowkey, List<String> fields, List<Object> cqlParams, final List<Object> eventList) throws DriverException, ImplementationExceptionResponse {
          LOG.debug("runQueryFromEventCF");
          PreparedStatement statement = session.prepare(sqlSelect);
          BoundStatement boundStatement = new BoundStatement(statement);
          
          int epc_position = 0;
          List<Integer> arrayPosition = new ArrayList<Integer>();
          List<List<?>> arrayPosition_value = new ArrayList<List<?>>();
          
          for (int i = 0; i < cqlParams.size(); i++) {     
              if ("epcList".equals(fields.get(i)) || "childEPCs".equals(fields.get(i)) || "epcClass".equals(fields.get(i)) || "parentID".equals(fields.get(i))) {
                  epc_position = i;
              } else if ("class java.lang.Long".equals(cqlParams.get(i).getClass().toString()))  {
                  boundStatement.setDate(i, new Date ((Long)cqlParams.get(i)));
              } else if (cqlParams.get(i) instanceof List<?>) {
                  List<?> values = (List<?>) cqlParams.get(i);
                  arrayPosition.add(i);
                  arrayPosition_value.add(values);
                  //boundStatement.setString(i, values.get(0).toString());
              } else {
                  boundStatement.setString(i, cqlParams.get(i).toString());
              }
          }
         
          List<Integer> valueSize = new ArrayList<Integer>(arrayPosition_value.size());
          for (List<?> list:arrayPosition_value) {
              valueSize.add(list.size());
          }
          
          listPositionArrayofStringValue = new ArrayList<List<Integer>>();
          getListPositionArrayofStringValue(new ArrayDeque<Integer>(), valueSize, valueSize.size());

          for (List<Integer> position:listPositionArrayofStringValue) {
              for (int i=0; i<position.size(); i++) {
                  LOG.debug(arrayPosition.get(i) + " : " + arrayPosition_value.get(i).get(position.get(i)).toString());
                  boundStatement.setString(arrayPosition.get(i), arrayPosition_value.get(i).get(position.get(i)).toString());
              }
              List<Row> listRowResult = new ArrayList<Row>();
              if (EpcisConstants.AGGREGATION_EVENT.equals(seQuery.getEventType())) {
                  if (rowkey != null) {
                      for (int i=0; i<rowkey.size(); i++) {                    
                          boundStatement.setString(epc_position, rowkey.get(i));
                          ResultSet results = session.execute(boundStatement);
                          for (Row row : results) {
                          	processResults(row, seQuery, eventList);
                          }
                      }                
                  } else {
                      ResultSet results = session.execute(boundStatement);
                      for (Row row : results) {
                      	processResults(row, seQuery, eventList);
                      }
                  }
              } else {
                  if (rowkey != null) {
                      for (int i=0; i<rowkey.size(); i++) {                    
                          boundStatement.setString(epc_position, rowkey.get(i));
                          ResultSet results = session.execute(boundStatement);
                          for (Row row : results) {
                              if (!isDuplicate(listRowResult, row)) {
                                  listRowResult.add(row);
                                  processResults(row, seQuery, eventList);
                              }
                          }
                      }                
                  } else {
                      ResultSet results = session.execute(boundStatement);
                      for (Row row : results) {
                          if (!isDuplicate(listRowResult, row)) {
                              listRowResult.add(row);
                              processResults(row, seQuery, eventList);
                          }
                      }
                  }
              }
          }
  }
    
    //Some rows may refer to one event (an event with a list of epc). Remove them before process result.
    private Boolean isDuplicate(List<Row> listRowResult, Row row) {        
        if (listRowResult.isEmpty()) return false;
        else {
            for (int i=0; i<listRowResult.size(); i++) {
                Row currentRow = listRowResult.get(i);
                LOG.debug("Current row eventTime: " + currentRow.getDate("eventTime"));
                LOG.debug("Row eventTime: " + row.getDate("eventTime"));
                LOG.debug("Equal?: " + currentRow.getDate("eventTime").equals(row.getDate("eventTime")));
                if (currentRow.getDate("eventTime").equals(row.getDate("eventTime")) &&
                     currentRow.getSet("epcset", String.class).contains(row.getString("epc").split("\\|")[0])) {
                    return true;
                }
            }
        }        
        return false;
    }
// recursive method to have a sequence of position list to support running multiple queries, e.g. (0,0,0), (0,0,1), (0,1,0) etc
    private static void getListPositionArrayofStringValue(Deque<Integer> indices, List<Integer> ranges, int n) {
        if (n != 0) {
           for (int i = 0; i < ranges.get(n-1); i++) {
              indices.push(i);
              getListPositionArrayofStringValue(indices, ranges, n-1);
              indices.pop();
           }
        }
        else {

           // inner most loop body, access to the index values thru indices
           List<Integer> out = Arrays.asList((Integer[])indices.toArray(new Integer[indices.size()]));
           listPositionArrayofStringValue.add(out);
                     
        }
    } 
    //Remove duplicate event, different epc rowkey but eventtime and epc list is the same
    private void processResults(Row row, SimpleEventQueryDTO seQuery, final List<Object> eventList) throws ImplementationExceptionResponse {
        LOG.debug("Process result!!");            
        Date eventTime = row.getDate("eventtime");
        long eventTimeMs = eventTime.getTime();

        Date recordTime = row.getDate("recordtime");
        long recordTimeMs = recordTime.getTime();
        Set<String> epcset = row.getSet("epcset", String.class);

        String readPointId = row.getString("readpoint");
        ReadPointType readPoint = null;
        if (readPointId != null) {
            readPoint = new ReadPointType();
            readPoint.setId(readPointId);
        }

        BusinessLocationType bizLocation = null;
        String bizLocationId = row.getString("bizlocation");
        if (bizLocationId != null) {
            bizLocation = new BusinessLocationType();
            bizLocation.setId(bizLocationId);
        }

        String bizStep = row.getString("bizstep");
        String disposition = row.getString("disposition");

        Map<String, String> biztransMap = row.getMap("biztransaction", String.class, String.class);
        BusinessTransactionListType bizlist = null;
        if (biztransMap.size() > 0) {
            Iterator<Entry<String, String>> iter = biztransMap.entrySet().iterator();
            bizlist = new BusinessTransactionListType();
            while (iter.hasNext()) {
                Map.Entry<String, String> mEntry = iter.next();
                BusinessTransactionType btrans = new BusinessTransactionType();
                btrans.setValue(mEntry.getValue());
                btrans.setType(mEntry.getKey());
                bizlist.getBizTransaction().add(btrans);
            }
        }
       

        EPCISEventType event = null;
        String eventType = seQuery.getEventType();
        if (EpcisConstants.AGGREGATION_EVENT.equals(eventType)) {
            AggregationEventType aggrEvent = new AggregationEventType();
            aggrEvent.setReadPoint(readPoint);
            aggrEvent.setBizLocation(bizLocation);
            aggrEvent.setBizStep(bizStep);
            aggrEvent.setDisposition(disposition);
            aggrEvent.setAction(ActionType.valueOf(row.getString("action")));
            aggrEvent.setParentID(row.getString("parentID").split("\\|")[0]);
            aggrEvent.setBizTransactionList(bizlist);
            EPCListType epcs = new EPCListType();                 
            for (String s : epcset) {
                EPC epc = new EPC();
                epc.setValue(s);
                epcs.getEpc().add(epc);
            }

            aggrEvent.setChildEPCs(epcs);
            readExtensionsFromResult(row, aggrEvent.getAny());
            event = aggrEvent;
        } else if (EpcisConstants.OBJECT_EVENT.equals(eventType)) {
            ObjectEventType objEvent = new ObjectEventType();
            objEvent.setReadPoint(readPoint);
            objEvent.setBizLocation(bizLocation);
            objEvent.setBizStep(bizStep);
            objEvent.setDisposition(disposition);
            objEvent.setAction(ActionType.valueOf(row.getString("action")));
            objEvent.setBizTransactionList(bizlist);
            EPCListType epcs = new EPCListType();                
            for (String s : epcset) {
                EPC epc = new EPC();
                epc.setValue(s);
                epcs.getEpc().add(epc);
            }
            objEvent.setEpcList(epcs);
            readExtensionsFromResult(row, objEvent.getAny());
            event = objEvent;
        } else if (EpcisConstants.QUANTITY_EVENT.equals(eventType)) {
            QuantityEventType quantEvent = new QuantityEventType();
            quantEvent.setReadPoint(readPoint);
            quantEvent.setBizLocation(bizLocation);
            quantEvent.setBizStep(bizStep);
            quantEvent.setDisposition(disposition);
            quantEvent.setEpcClass(row.getString("epcclass"));
            quantEvent.setQuantity(row.getInt("quantity"));
            quantEvent.setBizTransactionList(bizlist);
            event = quantEvent;
        }  else if (EpcisConstants.TRANSACTION_EVENT.equals(eventType)) {
            TransactionEventType transEvent = new TransactionEventType();
            transEvent.setReadPoint(readPoint);
            transEvent.setBizLocation(bizLocation);
            transEvent.setBizStep(bizStep);
            transEvent.setDisposition(disposition);
            transEvent.setAction(ActionType.valueOf(row.getString("action")));
            transEvent.setParentID(row.getString("parentID"));
            transEvent.setBizTransactionList(bizlist);
            EPCListType epcs = new EPCListType();                
            for (String s : epcset) {
                EPC epc = new EPC();
                epc.setValue(s);
                epcs.getEpc().add(epc);
            }
            event = transEvent;
        } else {
            String msg = "Unknown event type: " + eventType;
            LOG.error(msg);
            ImplementationException ie = new ImplementationException();
            ie.setReason(msg);
            throw new ImplementationExceptionResponse(msg, ie);
        }

        event.setEventTime(timeToXmlCalendar(eventTimeMs));
        event.setRecordTime(timeToXmlCalendar(recordTimeMs));
        event.setEventTimeZoneOffset(row.getString("timezone"));
        eventList.add(event);

    }
    
    private XMLGregorianCalendar timeToXmlCalendar(long time) throws ImplementationExceptionResponse {
        try {
            DatatypeFactory factory = DatatypeFactory.newInstance();
            Calendar cal = GregorianCalendar.getInstance();
            cal.setTimeInMillis(time);
            //cal.setTimeZone(TimeZone.getDefault());
            return factory.newXMLGregorianCalendar((GregorianCalendar) cal);
        } catch (DatatypeConfigurationException e) {
            String msg = "Unable to instantiate an XML representation for a date/time datatype.";
            ImplementationException iex = new ImplementationException();
            iex.setReason(msg);
            iex.setSeverity(ImplementationExceptionSeverity.SEVERE);
            throw new ImplementationExceptionResponse(msg, iex, e);
        }
    }
    
    private static List<Integer> parseTimestamp(String epoch) {
        List <Integer> list = new ArrayList<Integer>();
        Calendar calendar = Calendar.getInstance();
        long time = Long.valueOf(epoch);
        calendar.setTimeInMillis(time);
        int year = calendar.get(Calendar.YEAR);
        int month = calendar.get(Calendar.MONTH) + 1;
        list.add(year);
        list.add(month);
        return list;
        
    }
    
    private void readExtensionsFromResult(final Row row, final List<Object> extensions) {
        LOG.debug("Read extensions");
        Map<String, String> prefixMap = row.getMap("prefix", String.class, String.class);
        List <Definition> cf = row.getColumnDefinitions().asList();
        List<String> prefixlist = new ArrayList<String>(prefixMap.keySet());                    
        for (Definition def : cf)
            for (String prefix : prefixlist) {
                LOG.debug("prefix: " + prefix);
                String columnname = def.getName().toLowerCase();
                LOG.debug("column name: " + columnname);
                //convert prefix name to column name style, e.g. change . to dot_
                String prefixtemp = prefixMap.get(prefix).replace("#", "_").replace("://", "virgule_").replace(":","collon_").replace("/","slash_").replace(".", "dot_");
                if (columnname.startsWith(prefixtemp.toLowerCase())) {
                    String value = "";
                    try {
                        value = row.getString(columnname);
                    } catch (InvalidTypeException e) {
                        try {
                            value = row.getDate(columnname).toString();
                        } catch (InvalidTypeException e2) {
                            try {
                                value = String.valueOf(row.getInt(columnname));
                            } catch (InvalidTypeException e3) {
                                value = String.valueOf(row.getFloat(columnname));
                            }
                        }
                    }
                    String[] parts = columnname.split("_");
                    String localPart = parts[parts.length - 1];
                    String pre_namespace = columnname.substring(0, columnname.length()-localPart.length()-1);
                    String namespace = pre_namespace.replace("slash_","/").replace("virgule_", "://").replace("collon_",":").replace("dot_", ".");
                    LOG.debug("Value: " + value);
                    LOG.debug("localPart: " + localPart);
                    LOG.debug("namespace: " + namespace);
                    LOG.debug("prefix: " + prefix);
                    if (value != null) {
                        JAXBElement<String> elem = new JAXBElement<String>(new QName(namespace, localPart, prefix), String.class,
                                value);
                        extensions.add(elem);
                    }

                }
        }
    }
    
    
    private static List<String> middlePeriod(int low_year, int low_month, int high_year, int high_month) {
        List<String> list= new ArrayList<String>();
        while (low_year <= high_year) {
            String temp_month = String.valueOf(low_month);
            String temp_year = String.valueOf(low_year);
            String temp = temp_year + temp_month;
            list.add(temp);
            if ((low_year < high_year) && (low_month < 12))
                low_month++;
            else if ((low_year == high_year) && (low_month < high_month))
                low_month++;
            else {
                low_year++;
                low_month = 1;
            }
        }
        return list;
    }
    
    public void storeSupscriptions(final Session session, QueryParams queryParams, String dest,
            String subscrId, SubscriptionControls controls, String trigger, QuerySubscriptionScheduled newSubscription,
            String queryName, Schedule schedule) throws DriverException, ImplementationExceptionResponse {
        String insert = "INSERT INTO subscription (subscriptionid, "
                + "params, dest, sched, trigg, initialrecordingtime, "
                + "exportifempty, queryname, lastexecuted) VALUES " + "(?, ?, ?, ?, ?, ?, ?, ?, ?)";
        PreparedStatement stmt = session.prepare(insert);
        BoundStatement boundStatement = new BoundStatement(stmt);
        LOG.debug("Insert subscription");
        LOG.debug("QUERY: " + insert);
        try {
            boundStatement.setString(0, subscrId);
            LOG.debug("       query param 1: " + subscrId);

            ByteArrayOutputStream outStream = new ByteArrayOutputStream();
            ObjectOutput out = new ObjectOutputStream(outStream);
            out.writeObject(queryParams);
            ByteBuffer buff = ByteBuffer.wrap(outStream.toByteArray());
            boundStatement.setBytes(1, buff);


            boundStatement.setString(2, dest.toString());
            LOG.debug("       query param 3: " + dest);

            outStream = new ByteArrayOutputStream();
            out = new ObjectOutputStream(outStream);
            out.writeObject(schedule);
            buff = ByteBuffer.wrap(outStream.toByteArray());
            boundStatement.setBytes(3, buff);

            boundStatement.setString(4, trigger);
            LOG.debug("       query param 5: " + trigger);

            Calendar cal = newSubscription.getInitialRecordTime();
            
            boundStatement.setDate(5, cal.getTime());
            LOG.debug("       query param 6: " + cal.getTime());

            boundStatement.setBool(6, controls.isReportIfEmpty());
            LOG.debug("       query param 7: " + controls.isReportIfEmpty());

            boundStatement.setString(7, queryName);
            LOG.debug("       query param 8: " + queryName);

            boundStatement.setDate(8, cal.getTime());
            LOG.debug("       query param 9: " + cal.getTime());

            session.execute(boundStatement);
        } catch (IOException e) {
            String msg = "Unable to store the subscription to the database: " + e.getMessage();
            LOG.error(msg);
            ImplementationException iex = new ImplementationException();
            iex.setReason(msg);
            iex.setSeverity(ImplementationExceptionSeverity.ERROR);
            throw new ImplementationExceptionResponse(msg, iex, e);
        }
    }
    
    private List<BoundStatement> prepareMasterDataQuery(final Session session,
            MasterDataQueryDTO mdQuery) throws DriverException {
        List<BoundStatement> returnBoundStatements= new ArrayList<BoundStatement>();
        
        StringBuilder cqlSelectFrom = new StringBuilder("SELECT * FROM Masterdata");
        StringBuilder cqlWhereClause = new StringBuilder(" WHERE");        
        String cqlSelect = null;
        
        String columnAttrName = null;
        String attrValue = null;        
        boolean eqAttr = false;
        
        List<String> vocabularyEqNames = mdQuery.getVocabularyEqNames();
        List<String> vocabularyTypes = mdQuery.getVocabularyTypes();
        Map<String, List<String>> attributeNameAndValues = mdQuery.getAttributeNameAndValues();
        List<String> cqlParams = new ArrayList<String>();
        if (vocabularyEqNames != null && !vocabularyEqNames.isEmpty()) {
            cqlWhereClause.append(" itemkey IN ( ");
            for (int i = 0; i < vocabularyEqNames.size(); i++) {
                cqlWhereClause.append("?, ");
                cqlParams.add(vocabularyEqNames.get(i));
            }
            cqlWhereClause.delete(cqlWhereClause.length() - 2, cqlWhereClause.length());
            cqlWhereClause.append(" )");            
        }
        
        //LOG.debug("Vocab types: " + vocabularyTypes);
        // filter by attribute names and values
        if (attributeNameAndValues != null && !attributeNameAndValues.isEmpty()) {
            // only consider 1 attribute query.
            for (String attrName : attributeNameAndValues.keySet()) {
                columnAttrName = attrName.replace(":", "_");                
                List<String> attrValues = attributeNameAndValues.get(attrName);
                attrValue = attrValues.get(0);
                eqAttr = true;
            }            
        }
        
        if (vocabularyTypes != null && !vocabularyTypes.isEmpty()) {
            cqlWhereClause.append(" AND vocabtype = ? ");
            if (eqAttr) cqlWhereClause.append(" AND ").append(columnAttrName).append(" = ? ");
            cqlSelect = cqlSelectFrom.append(cqlWhereClause).toString().replace("WHERE AND", "WHERE");
            LOG.debug("Query Masterdata: " + cqlSelect);
            for (String vocabtype:vocabularyTypes) {
                PreparedStatement stmt = session.prepare(cqlSelect);
                BoundStatement boundStatement = new BoundStatement(stmt);
                for (int i=0; i<cqlParams.size(); i++) {
                    boundStatement.setString(i, cqlParams.get(i));
                }
                boundStatement.setString("vocabtype", vocabularyTablenameMap.get(vocabtype));
                if (eqAttr) boundStatement.setString(columnAttrName, attrValue);
                returnBoundStatements.add(boundStatement);     
            }           
        } else if (vocabularyEqNames != null && !vocabularyEqNames.isEmpty()) {
            if (eqAttr) cqlWhereClause.append(" AND ").append(columnAttrName).append(" = ? ");
            cqlSelect = cqlSelectFrom.append(cqlWhereClause).toString();
            LOG.debug("Query Masterdata: " + cqlSelect);
            PreparedStatement stmt = session.prepare(cqlSelect);
            BoundStatement boundStatement = new BoundStatement(stmt);
            for (int i=0; i<cqlParams.size(); i++) {
                boundStatement.setString(i, cqlParams.get(i));
            }
            if (eqAttr) boundStatement.setString(columnAttrName, attrValue);
            returnBoundStatements.add(boundStatement);            
        } else {
            if (eqAttr) cqlWhereClause.append(" AND ").append(columnAttrName).append(" = ? ");
            cqlSelect = cqlSelectFrom.append(cqlWhereClause).toString().replace("WHERE AND", "WHERE");;
            LOG.debug("Query Masterdata: " + cqlSelect);
            PreparedStatement stmt = session.prepare(cqlSelect);
            BoundStatement boundStatement = new BoundStatement(stmt);
            if (eqAttr) boundStatement.setString(columnAttrName, attrValue);
            returnBoundStatements.add(boundStatement);
     
        }                
        //LOG.debug("CQLSELECT: " + cqlSelect);

        return returnBoundStatements;
    }
    
    public void runMasterDataQuery(final Session session,
            MasterDataQueryDTO mdQuery, final List<VocabularyType> vocList) throws DriverException {        
        List<BoundStatement> boundStatements = prepareMasterDataQuery (session, mdQuery);
        for (BoundStatement boundStatement:boundStatements) {
            LOG.debug("Master query run !");
            ResultSet rs = session.execute(boundStatement);
            boolean includeAttributes = mdQuery.getIncludeAttributes();            
            List<AttributeType> attributes = new ArrayList<AttributeType>();            
            List <String> MasterdataColumnName = null;
            String vocabtype = null;
            Map<String, VocabularyElementListType> masterdataElemsTypeMap = new HashMap<String, VocabularyElementListType>();
            
            for (Row row : rs) {
                vocabtype = row.getString("vocabtype");
                VocabularyElementListType vocElems = null;
                if (masterdataElemsTypeMap.get(vocabtype) != null) {
                    vocElems = masterdataElemsTypeMap.get(vocabtype);
                } else {
                    vocElems = new VocabularyElementListType();
                }                
                                                
                VocabularyElementType vocElem = new VocabularyElementType();
                vocElem.setId(row.getString(0));                
                if (includeAttributes) {
                    if (mdQuery.getIncludedAttributeNames() != null) {
                        for (String str: mdQuery.getIncludedAttributeNames()) {                        
                            String column = str.replace(":", "_");
                            if (!row.isNull(column)) {
                                AttributeType attr = new AttributeType();
                                attr.setId(str);
                                attr.getOtherAttributes().put(new QName("value"), row.getString(column));
                                attributes.add(attr);
                                vocElem.getAttribute().add(attr);
                            }                            
                        }
                        
                    } else {
                        //read all attributes
                        if (MasterdataColumnName == null) {
                            List <Definition> cf = rs.getColumnDefinitions().asList();
                            MasterdataColumnName = new ArrayList<String>(cf.size());
                            for (Definition def : cf) {
                                if (!def.getName().equals("itemkey") && !def.getName().equals("vocabtype")) {
                                    MasterdataColumnName.add(def.getName()); 
                                }
                            }
                        }                 
                        for (String str: MasterdataColumnName) {                        
                            if (!row.isNull(str)) {
                                AttributeType attr = new AttributeType();
                                attr.setId(str.replace("_", ":"));
                                attr.getOtherAttributes().put(new QName("value"), row.getString(str));
                                attributes.add(attr);
                                vocElem.getAttribute().add(attr);
                            }
                        }
                    }
                }
                vocElems.getVocabularyElement().add(vocElem);
                masterdataElemsTypeMap.put(vocabtype, vocElems);
            }
            
            Iterator<String> keySetIterator = masterdataElemsTypeMap.keySet().iterator();
            while(keySetIterator.hasNext()){
              String voctype = keySetIterator.next();
              VocabularyType voc = new VocabularyType();
              voc.setType(vocabularyTablenameMap_Inversed.get(voctype));
              voc.setVocabularyElementList(masterdataElemsTypeMap.get(voctype));
              vocList.add(voc);
            }
        }
    }
    
    public Map<String, QuerySubscriptionScheduled> fetchSubscriptions(final Session session)
            throws DriverException, ImplementationExceptionResponse {
        String query = "SELECT * FROM subscription";
        LOG.debug("CQL: " + query);
        ResultSet rs = session.execute(query);
        QuerySubscriptionScheduled storedSubscription;
        GregorianCalendar initrectime = new GregorianCalendar();
        Map<String, QuerySubscriptionScheduled> subscribedMap = new HashMap<String, QuerySubscriptionScheduled>();
        for (Row row : rs) {
            try {
                String subscrId = row.getString("subscriptionid");                
                ByteBuffer buff = row.getBytes("params");
                LOG.debug("1 " + subscrId);
                byte[] byts = new byte[buff.remaining()];
                buff.get(byts);
                LOG.debug("2 ");
                ObjectInputStream istream = new ObjectInputStream(new ByteArrayInputStream(byts));
                LOG.debug("3 ");
                QueryParams params = (QueryParams) istream.readObject();
                LOG.debug("4 ");
                
                LOG.debug(params.getClass().toString());

                String dest = row.getString("dest");

                buff = row.getBytes("sched");
                byts = new byte[buff.remaining()];
                buff.get(byts);
                istream = new ObjectInputStream(new ByteArrayInputStream(byts));
                Schedule sched = (Schedule) istream.readObject();

                initrectime.setTime(row.getDate("initialrecordingtime"));

                boolean exportifempty = row.getBool("exportifempty");

                String queryName = row.getString("queryname");
                String trigger = row.getString("trigg");

                if (trigger == null || trigger.length() == 0) {
                    storedSubscription = new QuerySubscriptionScheduled(subscrId, params, dest,
                            Boolean.valueOf(exportifempty), initrectime, new GregorianCalendar(), sched, queryName);
                } else {
                    storedSubscription = new QuerySubscriptionTriggered(subscrId, params, dest,
                            Boolean.valueOf(exportifempty), initrectime, new GregorianCalendar(), queryName, trigger,
                            sched);
                }
                subscribedMap.put(subscrId, storedSubscription);
            } catch (DriverException e) {
                // sql exceptions are passed on
                throw e;
            } catch (Exception e) {
                // all other exceptions are caught
                String msg = "Unable to restore subscribed queries from the database.";
                LOG.error(msg, e);
                ImplementationException iex = new ImplementationException();
                iex.setReason(msg);
                iex.setSeverity(ImplementationExceptionSeverity.ERROR);
                throw new ImplementationExceptionResponse(msg, iex, e);
            }
        }
        return subscribedMap;
    }
    
    public boolean fetchExistsSubscriptionId(final Session session, final String subscriptionID)
            throws DriverException {
        String query = "SELECT subscriptionid FROM Subscription WHERE subscriptionid = ?";
        PreparedStatement stmt = session.prepare(query);
        BoundStatement boundStatement = new BoundStatement(stmt);
        boundStatement.setString(0, subscriptionID);
        if (LOG.isDebugEnabled()) {
            LOG.debug("CQL: " + query);
            LOG.debug("     param1 = " + subscriptionID);
        }
        ResultSet rs = session.execute(boundStatement);
        if (rs.one() != null) return true;
        else return false;
    }
    
    public void deleteSubscription(final Session session, String subscrId) throws DriverException {
        String delete = "DELETE FROM subscription WHERE subscriptionid=?";
        PreparedStatement stmt = session.prepare(delete);
        BoundStatement boundStatement = new BoundStatement(stmt);
        boundStatement.setString(0, subscrId);
        if (LOG.isDebugEnabled()) {
            LOG.debug("CQL: " + delete);
            LOG.debug("     param1 = " + subscrId);
        }
        session.execute(boundStatement);
    }
    
    public Boolean getAllowFiltering() {
        return allowFiltering;
    }

    public void setAllowFiltering(Boolean allowFiltering) {
        this.allowFiltering = allowFiltering;
    }
}
