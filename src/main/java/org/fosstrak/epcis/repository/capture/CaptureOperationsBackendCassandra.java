package org.fosstrak.epcis.repository.capture;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.fosstrak.epcis.repository.model.EventFieldExtension;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ColumnDefinitions.Definition;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.*;

/**
 * This class uses Cassandra Query Language and Datastax Cassandra Java Driver
 * for inserting EPCIS events into the EPCIS repository.
 * 
 * @author TuanLe
 * 
 */
public class CaptureOperationsBackendCassandra {

	private static final Log LOG = LogFactory
			.getLog(CaptureOperationsBackendCassandra.class);
	private static final String CQL_INSERT_OBJECTEVENT = "INSERT INTO ObjectEvent (epc, eventtime, bizstep, disposition, readpoint, action, bizlocation, biztransaction, timezone, epcset, recordtime, prefix)";
	private static final String CQL_INSERT_OBJECTEVENT_VALUES = " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)";
	private static final String CQL_INSERT_AGGREGATIONEVENT = "INSERT INTO AggregationEvent (eventtime, bizstep, disposition, readpoint, action, parentid, bizlocation, biztransaction, timezone, epcset, recordtime, prefix)";
	private static final String CQL_INSERT_AGGREGATIONEVENT_VALUES = " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
	private static final String CQL_INSERT_AGGREGATION_CHILDEPC = "INSERT INTO EPCIndex_AggregationEvent (childepc, eventtime, parentid) VALUES (?, ?, ?);";

	private static final String CQL_INSERT_QUANTITYEVENT = "INSERT INTO QuantityEvent (epcclass, eventtime, bizstep, disposition, readpoint, quantity, bizlocation, biztransaction, timezone, recordtime, prefix)";
	private static final String CQL_INSERT_QUANTITYEVENT_VALUES = " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

	private static final String CQL_INSERT_TRANSACTIONEVENT = "INSERT INTO TransactionEvent (epc, eventtime, bizstep, disposition, readpoint, bizlocation, biztransaction, timezone, epcset, recordtime, prefix)";
	private static final String CQL_INSERT_TRANSACTIONEVENT_VALUES = " VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";

	private static final String CQL_DELETE_MASTERDATA = "DELETE FROM MasterData WHERE itemkey = ?;";

	private static List<String> MasterdataColumnName;
	private static List<String> ObjectEventColumnName;
	private static List<String> AggregationEventColumnName;
	private static List<String> QuantityEventColumnName;
	private static List<String> TransactionEventColumnName;

	public void insertObjectEvent(final Session session, final String epc,
			final Date eventTime, final String bizStepId,
			final String dispositionId, final String readPointId,
			final String action, final String bizlocation,
			final Map<String, String> biztransMap, final String timezone,
			final Set<String> epcset, final Date recordTime,
			final List<EventFieldExtension> fieldNameExtList)
			throws DriverException {
		String query = "";
		Map<String, String> prefixPairs = null;
		try {
			if (fieldNameExtList.isEmpty()) {
				query = CQL_INSERT_OBJECTEVENT + CQL_INSERT_OBJECTEVENT_VALUES;
			} else {
				StringBuilder insertinto = new StringBuilder(
						CQL_INSERT_OBJECTEVENT);
				StringBuilder insertvalue = new StringBuilder(
						CQL_INSERT_OBJECTEVENT_VALUES);
				insertinto.delete(insertinto.length() - 1, insertinto.length());
				insertvalue.delete(insertvalue.length() - 1,
						insertvalue.length());
				prefixPairs = new HashMap<String, String>();
				for (EventFieldExtension ext : fieldNameExtList) {
					handleExtensionFields(session, "ObjectEvent", ext);
					String extcolumnname = ext.getFieldname().replace("#", "_")
							.replace("://", "virgule_").replace(":", "collon_")
							.replace("/", "slash_").replace(".", "dot_");
					insertinto.append(", " + extcolumnname);
					insertvalue.append(", ?");
					prefixPairs.put(ext.getPrefix(), ext.getNamespace());
				}
				insertinto.append(")");
				insertvalue.append(")");
				query = insertinto.append(insertvalue).toString();
			}
			LOG.debug("Insert objectEvent: " + query);
			PreparedStatement statement = session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.setString(0, epc);
			boundStatement.setDate(1, eventTime);
			boundStatement.setString(2, bizStepId);
			boundStatement.setString(3, dispositionId);
			boundStatement.setString(4, readPointId);
			boundStatement.setString(5, action);
			boundStatement.setString(6, bizlocation);
			boundStatement.setMap(7, biztransMap);
			boundStatement.setString(8, timezone);
			boundStatement.setSet(9, epcset);
			boundStatement.setDate(10, recordTime);
			boundStatement.setMap(11, prefixPairs);
			if (!fieldNameExtList.isEmpty()) {
				for (EventFieldExtension ext : fieldNameExtList) {
					String extcolumnname = ext.getFieldname().replace("#", "_")
							.replace("://", "virgule_").replace(":", "collon_")
							.replace("/", "slash_").replace(".", "dot_");
					if (ext.getIntValue() != null) {
						boundStatement.setInt(extcolumnname, ext.getIntValue());
					} else if (ext.getFloatValue() != null) {
						boundStatement.setFloat(extcolumnname,
								ext.getFloatValue());
					} else if (ext.getDateValue() != null) {
						boundStatement.setDate(extcolumnname,
								ext.getDateValue());
					} else {
						boundStatement.setString(extcolumnname,
								ext.getStrValue());
					}
				}
			}
			session.execute(boundStatement);

		} catch (DriverException e) {
			e.printStackTrace();
			throw e;
		}

	}

	// public void insertLocationIndex_ObjectEvent(final Session session, final
	// Set<String> epcset, final Date eventTime,
	// final String bizlocation) throws DriverException {
	//
	// LOG.debug("Insert LocationIndex_ObjectEvent");
	// try {
	//
	// PreparedStatement statement =
	// session.prepare(CQL_INSERT_LOCATION_INDEX_OBJECTEVENT);
	// BoundStatement boundStatement = new BoundStatement(statement);
	// boundStatement.setSet(0, epcset);
	// boundStatement.setDate(1, eventTime);
	// boundStatement.setString(2, bizlocation);
	// session.execute(boundStatement);
	// } catch (DriverException e) {
	// e.printStackTrace();
	// throw e;
	// }
	// }

	public void insertAggregationEvent(final Session session,
			final Date eventTime, final String bizStepId,
			final String dispositionId, final String readPointId,
			final String action, final String parentid, final String location,
			final Map<String, String> biztransMap, final String timezone,
			final Set<String> epcset, final Date recordTime,
			final List<EventFieldExtension> fieldNameExtList)
			throws DriverException {
		String query = "";
		LOG.debug("Insert AggregationEvent");
		Map<String, String> prefixPairs = null;
		try {
			if (fieldNameExtList.isEmpty()) {
				query = CQL_INSERT_AGGREGATIONEVENT
						+ CQL_INSERT_AGGREGATIONEVENT_VALUES;
			} else {
				StringBuilder insertinto = new StringBuilder(
						CQL_INSERT_AGGREGATIONEVENT);
				StringBuilder insertvalue = new StringBuilder(
						CQL_INSERT_AGGREGATIONEVENT_VALUES);
				insertinto.delete(insertinto.length() - 1, insertinto.length());
				insertvalue.delete(insertvalue.length() - 1,
						insertvalue.length());
				prefixPairs = new HashMap<String, String>();
				for (EventFieldExtension ext : fieldNameExtList) {
					handleExtensionFields(session, "AggregationEvent", ext);
					String extcolumnname = ext.getFieldname().replace("#", "_")
							.replace("://", "virgule_").replace(":", "collon_")
							.replace("/", "slash_").replace(".", "dot_");
					insertinto.append(", " + extcolumnname);
					insertvalue.append(", ?");
					prefixPairs.put(ext.getPrefix(), ext.getNamespace());
				}
				insertinto.append(")");
				insertvalue.append(")");
				query = insertinto.append(insertvalue).toString();
			}
			LOG.debug("Query: " + query);
			LOG.debug("Insert AggregationEvent: " + query);
			PreparedStatement statement = session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.setDate(0, eventTime);
			boundStatement.setString(1, bizStepId);
			boundStatement.setString(2, dispositionId);
			boundStatement.setString(3, readPointId);
			boundStatement.setString(4, action);
			boundStatement.setString(5, parentid);
			boundStatement.setString(6, location);
			boundStatement.setMap(7, biztransMap);
			boundStatement.setString(8, timezone);
			boundStatement.setSet(9, epcset);
			boundStatement.setDate(10, recordTime);
			boundStatement.setMap(11, prefixPairs);
			session.execute(boundStatement);
		} catch (DriverException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public void insertAggregationChildepc(final Session session,
			final String childepc, final Date eventTime, final String parentid) {
		LOG.debug("Insert Aggregation ChildEPC");
		try {
			PreparedStatement statement = session
					.prepare(CQL_INSERT_AGGREGATION_CHILDEPC);
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.setString(0, childepc);
			boundStatement.setDate(1, eventTime);
			boundStatement.setString(2, parentid);
			session.execute(boundStatement);
		} catch (DriverException e) {
			e.printStackTrace();
			throw e;
		}

	}

	public void insertQuantityEvent(final Session session,
			final String epcclass, final Date eventTime,
			final String bizStepId, final String dispositionId,
			final String readPointId, final int quantity,
			final String bizlocation, final Map<String, String> biztransMap,
			final String timezone, final Date recordTime,
			final List<EventFieldExtension> fieldNameExtList)
			throws DriverException {
		String query = "";
		LOG.debug("Insert QuantityEvent");
		Map<String, String> prefixPairs = null;
		try {
			if (fieldNameExtList.isEmpty()) {
				query = CQL_INSERT_QUANTITYEVENT
						+ CQL_INSERT_QUANTITYEVENT_VALUES;
			} else {
				StringBuilder insertinto = new StringBuilder(
						CQL_INSERT_QUANTITYEVENT);
				StringBuilder insertvalue = new StringBuilder(
						CQL_INSERT_QUANTITYEVENT_VALUES);
				insertinto.delete(insertinto.length() - 1, insertinto.length());
				insertvalue.delete(insertvalue.length() - 1,
						insertvalue.length());
				prefixPairs = new HashMap<String, String>();
				for (EventFieldExtension ext : fieldNameExtList) {
					handleExtensionFields(session, "QuantityEvent", ext);
					String extcolumnname = ext.getFieldname().replace("#", "_")
							.replace("://", "virgule_").replace(":", "collon_")
							.replace("/", "slash_").replace(".", "dot_");
					insertinto.append(", " + extcolumnname);
					insertvalue.append(", ?");
					prefixPairs.put(ext.getPrefix(), ext.getNamespace());
				}
				insertinto.append(")");
				insertvalue.append(")");
				query = insertinto.append(insertvalue).toString();
			}
			LOG.debug("Query: " + query);
			LOG.debug("Insert QuantityEvent: " + query);
			PreparedStatement statement = session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.setString(0, epcclass);
			boundStatement.setDate(1, eventTime);
			boundStatement.setString(2, bizStepId);
			boundStatement.setString(3, dispositionId);
			boundStatement.setString(4, readPointId);
			boundStatement.setInt(5, quantity);
			boundStatement.setString(6, bizlocation);
			boundStatement.setMap(7, biztransMap);
			boundStatement.setString(8, timezone);
			boundStatement.setDate(9, recordTime);
			boundStatement.setMap(10, prefixPairs);
			session.execute(boundStatement);
		} catch (DriverException e) {
			e.printStackTrace();
			throw e;
		}

	}

	public void insertTransactionEvent(final Session session, final String epc,
			final Date eventTime, final String bizStepId,
			final String dispositionId, final String readPointId,
			final String bizlocation, final Map<String, String> biztransMap,
			final String timezone, final Set<String> epcset,
			final Date recordTime,
			final List<EventFieldExtension> fieldNameExtList)
			throws DriverException {
		String query = "";
		LOG.debug("Insert TransactionEvent");
		Map<String, String> prefixPairs = null;
		try {
			if (fieldNameExtList.isEmpty()) {
				query = CQL_INSERT_TRANSACTIONEVENT
						+ CQL_INSERT_TRANSACTIONEVENT_VALUES;
			} else {
				StringBuilder insertinto = new StringBuilder(
						CQL_INSERT_TRANSACTIONEVENT);
				StringBuilder insertvalue = new StringBuilder(
						CQL_INSERT_TRANSACTIONEVENT_VALUES);
				insertinto.delete(insertinto.length() - 1, insertinto.length());
				insertvalue.delete(insertvalue.length() - 1,
						insertvalue.length());
				prefixPairs = new HashMap<String, String>();
				for (EventFieldExtension ext : fieldNameExtList) {
					handleExtensionFields(session, "TransactionEvent", ext);
					String extcolumnname = ext.getFieldname().replace("#", "_")
							.replace("://", "virgule_").replace(":", "collon_")
							.replace("/", "slash_").replace(".", "dot_");
					insertinto.append(", " + extcolumnname);
					insertvalue.append(", ?");
					prefixPairs.put(ext.getPrefix(), ext.getNamespace());
				}
				insertinto.append(")");
				insertvalue.append(")");
				query = insertinto.append(insertvalue).toString();
			}
			LOG.debug("Query: " + query);
			LOG.debug("Insert TransactionEvent: " + query);
			PreparedStatement statement = session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.setString(0, epc);
			boundStatement.setDate(1, eventTime);
			boundStatement.setString(2, bizStepId);
			boundStatement.setString(3, dispositionId);
			boundStatement.setString(4, readPointId);
			boundStatement.setString(5, bizlocation);
			boundStatement.setMap(6, biztransMap);
			boundStatement.setString(7, timezone);
			boundStatement.setSet(8, epcset);
			boundStatement.setDate(9, recordTime);
			boundStatement.setMap(10, prefixPairs);
			if (!fieldNameExtList.isEmpty()) {
				for (EventFieldExtension ext : fieldNameExtList) {
					String extcolumnname = ext.getFieldname().replace("#", "_")
							.replace("://", "virgule_").replace(":", "collon_")
							.replace("/", "slash_").replace(".", "dot_");
					if (ext.getIntValue() != null) {
						boundStatement.setInt(extcolumnname, ext.getIntValue());
					} else if (ext.getFloatValue() != null) {
						boundStatement.setFloat(extcolumnname,
								ext.getFloatValue());
					} else if (ext.getDateValue() != null) {
						boundStatement.setDate(extcolumnname,
								ext.getDateValue());
					} else {
						boundStatement.setString(extcolumnname,
								ext.getStrValue());
					}
				}
			}
			session.execute(boundStatement);
		} catch (DriverException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public void insertMasterdata(final Session session, final String epc,
			final Map<String, String> nmap, String vocType, String editmode) {

		LOG.debug("Insert Masterdata_Flexible");
		try {
			// MasterdataColumnName keeps track of columns name in Masterdata
			if (MasterdataColumnName == null) {
				LOG.debug("Masterdatacolumn is null");
				ResultSet rs = session
						.execute("SELECT * FROM MASTERDATA WHERE itemkey = 'TESTKEY';");
				List<Definition> cf = rs.getColumnDefinitions().asList();
				MasterdataColumnName = new ArrayList<String>(cf.size());
				for (Definition def : cf) {
					MasterdataColumnName.add(def.getName().toLowerCase());
				}
			} else {
				for (int i = 0; i < MasterdataColumnName.size(); i++) {
					LOG.debug("Masterdata column: "
							+ MasterdataColumnName.get(i));
				}
			}

			StringBuilder insert = new StringBuilder(
					"INSERT INTO MASTERDATA (itemkey, vocabtype, ");
			StringBuilder value = new StringBuilder(" VALUES (?, ?, ");
			Set<String> mapkey = nmap.keySet();

			// Go through all key that need to be added
			for (String temp : mapkey) {
				if (!MasterdataColumnName.contains(temp.toLowerCase())) {
					session.execute("ALTER TABLE masterdata ADD " + temp
							+ " text;");
					MasterdataColumnName.add(temp);
				}
				insert.append(temp + ", ");
				value.append("?, ");
			}

			// IF insert - reset other columns to null. If update, pass over.
			if (!editmode.equals("2")) {
				for (int i = 0; i < MasterdataColumnName.size(); i++) {
					if (!mapkey.contains(MasterdataColumnName.get(i))
							&& !MasterdataColumnName.get(i).equals("itemkey")
							&& !MasterdataColumnName.get(i).equals("vocabtype")) {
						insert.append(MasterdataColumnName.get(i) + ", ");
						value.append("null, ");
					}
				}
			}

			insert.delete(insert.length() - 2, insert.length());
			insert.append(")");

			value.delete(value.length() - 2, value.length());
			value.append(")");

			String query = insert.append(value).toString();
			LOG.debug("Query: " + query);
			PreparedStatement statement = session.prepare(query);
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.setString(0, epc);
			boundStatement.setString(1, vocType);
			int count = 2;
			for (String temp : mapkey) {
				boundStatement.setString(count, nmap.get(temp));
				count++;
			}

			session.execute(boundStatement);

		} catch (DriverException e) {
			e.printStackTrace();
			throw e;
		}
	}

	public void deleteMasterdata(final Session session, final String epc_key) {

		LOG.debug("Delete Cassandra Masterdata");
		try {
			PreparedStatement statement = session
					.prepare(CQL_DELETE_MASTERDATA);
			BoundStatement boundStatement = new BoundStatement(statement);
			boundStatement.setString(0, epc_key);
			session.execute(boundStatement);
		} catch (DriverException e) {
			e.printStackTrace();
			throw e;
		}
	}

	private void handleExtensionFields(final Session session, String eventType,
			EventFieldExtension ext) {
		String fieldname_column = ext.getFieldname().replace("#", "_")
				.replace("://", "virgule_").replace(":", "collon_")
				.replace("/", "slash_").replace(".", "dot_");
		LOG.debug("Field name: " + ext.getFieldname());
		List<String> currentColumnname = getColumnName(session, eventType);
		if (currentColumnname.contains(fieldname_column.toLowerCase())) {
			LOG.debug("Already has this column name");
			return;
		} else {
			String valueType = null;
			if (ext.getIntValue() != null) {
				valueType = "int";
			} else if (ext.getFloatValue() != null) {
				valueType = "float";
			} else if (ext.getDateValue() != null) {
				valueType = "timestamp";
			} else {
				valueType = "text";
			}
			LOG.debug(fieldname_column);
			LOG.debug("ALTER TABLE " + eventType + " ADD " + fieldname_column
					+ " " + valueType);
			session.execute("ALTER TABLE " + eventType + " ADD "
					+ fieldname_column + " " + valueType);
			session.execute("CREATE INDEX ON " + eventType + "("
					+ fieldname_column + ")");
			if (eventType.equals("ObjectEvent")) {
				ObjectEventColumnName.add(fieldname_column.toLowerCase());
			} else if (eventType.equals("QuantityEvent")) {
				QuantityEventColumnName.add(fieldname_column.toLowerCase());
			} else if (eventType.equals("TransactionEvent")) {
				TransactionEventColumnName.add(fieldname_column.toLowerCase());
			} else if (eventType.equals("AggregationEvent")) {
				AggregationEventColumnName.add(fieldname_column.toLowerCase());
			}
		}
	}

	private List<String> getColumnName(final Session session,
			final String eventType) {
		List<String> currentlist = null;
		if (eventType.equals("ObjectEvent")) {
			currentlist = ObjectEventColumnName;
		} else if (eventType.equals("QuantityEvent")) {
			currentlist = QuantityEventColumnName;
		} else if (eventType.equals("TransactionEvent")) {
			currentlist = TransactionEventColumnName;
		} else if (eventType.equals("AggregationEvent")) {
			currentlist = AggregationEventColumnName;
		}
		if (currentlist == null) {
			LOG.debug(eventType + " column is null");
			ResultSet rs = session.execute("SELECT * FROM " + eventType
					+ " WHERE epc = 'TESTKEY';");
			List<Definition> cf = rs.getColumnDefinitions().asList();
			currentlist = new ArrayList<String>(cf.size());
			for (Definition def : cf) {
				currentlist.add(def.getName().toLowerCase());
			}
		}

		if (eventType.equals("ObjectEvent")) {
			ObjectEventColumnName = currentlist;
		} else if (eventType.equals("QuantityEvent")) {
			QuantityEventColumnName = currentlist;
		} else if (eventType.equals("TransactionEvent")) {
			TransactionEventColumnName = currentlist;
		} else if (eventType.equals("AggregationEvent")) {
			AggregationEventColumnName = currentlist;
		}

		return currentlist;
	}

	// get Latitude, Longtitude from Masterdata
	public List<String> getGeoLocation(final Session session,
			final String location) {
		List<String> geoLocation = null;
		String geoQuery = "SELECT urn_epcglobal_fmcg_mda_longitude, urn_epcglobal_fmcg_mda_latitude FROM masterdata WHERE itemkey = '"
				+ location + "';";
		ResultSet rs = session.execute(geoQuery);
		for (Row row : rs) {
			geoLocation = new ArrayList<String>(2);
			geoLocation.add(row.getString(0));
			geoLocation.add(row.getString(1));
		}
		return geoLocation;

	}

}
