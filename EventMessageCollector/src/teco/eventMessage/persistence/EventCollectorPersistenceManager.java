/**
 * Persistence management for hole application.
 * 
 */
package teco.eventMessage.persistence;

import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import teco.eventMessage.EventMessage;
import teco.eventMessage.EventMessageConfigurationException;
import teco.eventMessage.EventMessageException;
import teco.eventMessage.Log;
import teco.eventMessage.Utils;
import teco.eventMessage.collector.EventCollectorGroup;
import teco.eventMessage.operation.CreateOperation;
import teco.eventMessage.operation.DeleteOperation;
import teco.eventMessage.operation.UpdateOperation;
import teco.eventMessage.origin.EventMessageOrigin;
import teco.eventMessage.processor.result.ProcessingResult;
import teco.eventMessage.processor.target.EventMessageTarget;
import teco.eventMessage.processor.target.EventMessageTargetToken;
import teco.eventMessage.processor.target.EventMessageTragetException;
import teco.eventMessage.retryer.OnErrorEventMessageNotFound;

/**
 * This class is responsible of persistence for all entities.
 * 
 * @author u190438
 *
 */
public class EventCollectorPersistenceManager {
	/* EventMessage queries */
	static private String db_event_insertQuery = "insert into EventMessage "
			+ "(eventMessageOriginId, eventMessageOperationId, transactionId, identification, type, publishDate, dequeuedDate, updatedDate, trxid, state, source) values "
			+ "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?);";
	static private String db_event_updateQuery = "UPDATE EventMessage, EventCollectorGroup "
			+ "SET EventMessage.state=?, EventMessage.processingInfo=?, EventMessage.updatedDate=?, "
			+ "EventCollectorGroup.retryableEventMessageId=?, EventCollectorGroup.lastExecutedEventMessageId=?, EventCollectorGroup.updatedTimestamp=? "
			+ "WHERE EventMessage.id=? AND EventCollectorGroup.id=?;";
	static private String db_event_single_updateQuery = "UPDATE EventMessage "
			+ "SET state=?, processingInfo=?, updatedDate=? "
			+ "WHERE id=?;";

	static private String db_event_retrieveIncludingQuery = "SELECT id, eventMessageOriginId, eventMessageOperationId, transactionId, identification, type, publishDate, "
			+ "dequeuedDate, updatedDate, trxid, state, processingInfo, source "
			+ "FROM `EventMessage` WHERE id >= ? AND state IN ('pending', 'retryable') AND eventMessageOriginId IN (";
	static private String db_event_retrieveAfterQuery = "SELECT id, eventMessageOriginId, eventMessageOperationId, transactionId, identification, type, publishDate, " 
			+ "dequeuedDate, updatedDate, trxid, state, processingInfo, source " 
			+ "FROM `EventMessage` WHERE id > ? AND state = 'pending' AND eventMessageOriginId IN (";
	static private String db_event_retrieveOneQuery = "SELECT id, eventMessageOriginId, eventMessageOperationId, transactionId, identification, type, publishDate, " 
			+ "dequeuedDate, updatedDate, trxid, state, processingInfo, source " 
			+ "FROM `EventMessage` WHERE id = ?";
	 
	/* EventCollectoGroup queries */
	static private String db_collectorGroup_retrieveQuery = "SELECT id, retryableEventMessageId, lastExecutedEventMessageId, failedEventsRetryableSeconds, updatedTimestamp "
			+ "FROM EventCollectorGroup WHERE name = ? limit 1;";

	/* EventMessageTarget queries */
	static private String db_target_retrieveQuery = "SELECT DISTINCT(t.id), t.name FROM EventMessageTarget t INNER JOIN EventMessageOrigin o ON t.id = o.eventMessageTargetId WHERE o.eventCollectorGroupId = ?;";

	/* EventMessageOperation queries */
	static private String db_operation_retrieveQuery = "SELECT id, name, operationType FROM EventMessageOperation;";

	/* EventMessageConfig queries */
	static private String db_config_retrieveQuery = "SELECT property, value FROM EventMessageConfig WHERE ownerId = ? AND ownerType = ?;";

	/* EventMessageOrigin queries */
	static private String db_eventMessageOrigin_retrieveQuery = "SELECT id, name, eventMessageTargetId, eventCollectorGroupOrder "
			+ "FROM EventMessageOrigin WHERE eventCollectorGroupId = ?;";

	/* EventCollectorRetryer related queries */
	static private String db_eventMessage_onErrorInitialID_query = "SELECT MIN(id) FROM EventMessage " + 
			"WHERE updatedDate >= DATE_SUB(CURRENT_DATE, INTERVAL ? SECOND) AND state = 'error' AND eventMessageOriginId IN (ORIGIN_IDS)";
	static private String db_eventMessage_idsByElement_query = "select identification, min(id) " + 
			"from EventMessage " + 
			"where state='error' AND id >= ? AND eventMessageOriginId IN (ORIGIN_IDS) " + 
			"group by identification;";
	static private String db_eventMessage_okMessageIdAfter_query = "SELECT min(id) FROM EventMessage " +
			"WHERE id > ? AND identification = ? AND eventMessageOriginId = ? AND state IN ('ok','warning') ORDER BY id ASC;";
	static private String db_eventMessage_updateAsObsolet_query = "UPDATE EventMessage " + 
			"SET state = 'obsolet' " + 
			"WHERE identification = ? AND id < ? AND id >= ? AND state = 'error' AND eventMessageOriginId = ?;";
	static private String db_eventMessage_onError_retrieveQuery = "SELECT id, eventMessageOriginId, eventMessageOperationId, transactionId, identification, type, publishDate, " + 
			"dequeuedDate, updatedDate, trxid, state, processingInfo, source " + 
			"FROM `EventMessage` WHERE id >= ? AND state = 'error' AND eventMessageOriginId IN (ORIGIN_IDS);";

	private PreparedStatement eventInsertStmt;
	private PreparedStatement eventUpdateStmt;
	private PreparedStatement eventRetrieveOneStmt;
	private PreparedStatement eventSingleUpdateStmt;
	private PreparedStatement collectorGroupRetrieveStmt;
	private PreparedStatement eventMessageTargetRetrieveStmt;
	private PreparedStatement eventMessageOperationRetrieveStmt;
	private PreparedStatement eventMessageConfigRetrieveStmt;
	private PreparedStatement eventMessageOriginRetrieveStmt;
	private PreparedStatement eventMessageOkMessageIdAfterStmt;
	private PreparedStatement eventMessageUpdateAsObsoletStmt;

	private String db_driver;
	private String db_url;
	private String db_user;
	private String db_password;
	private Long max_processable_events;
	private Connection db_conn;
	
	private static Map<Long, EventMessageTarget> targetsByIdMap;
	private static EventCollectorPersistenceManager instance;

	/**
	 * Initialize the single instance with properties passed.
	 * If there is a previous instance release it before create the new one.
	 * 
	 * @param configProp, contain properties needed for instance initialization.
	 * @throws Exception, if one parameter is missing or fails the db connection.
	 */
	public static void initialize(Properties configProp) throws Exception {
		String driver = configProp.getProperty("db_driver");
		String url = configProp.getProperty("db_url");
		String user = configProp.getProperty("db_user");
		String password = configProp.getProperty("db_password");
		String max_events = configProp.getProperty("db_max_processable_events");
		
		if (driver == null || url == null || user == null || password == null)
			throw new EventMessageConfigurationException("Required configuration property missing!");

		releaseInstance();
		instance = new EventCollectorPersistenceManager(driver, url, user, password, max_events);
	}

	public static EventCollectorPersistenceManager getInstance() {
		return instance;
	}

	public static void releaseInstance() {
		if (instance != null)
			instance.release();
	}

	/******************************************************************************
	 * 
	 * Entities store interface
	 * 
	 ******************************************************************************/

	/**
	 * Store a new EventMessage in the database.
	 * If success set the generated ID and update timestamp to the event.
	 * 
	 * @param event, a non persistent EventMessage recently created
	 */
	public void saveEvent(EventMessage event) throws EventMessagePersistenceException {
		LocalDateTime updateTime = LocalDateTime.now();
		Timestamp updateTimestamp = Timestamp.valueOf(updateTime);
		long eventId;
		try {
			/* 1-eventMessageOriginId, 2-eventMessageOperationId, 3-transactionId, 4-identification, 5-type, 6-publishDate,
			 * 7-dequeuedDate, 8-updatedDate, 9-trxid, 10-state, 11-source */
			eventInsertStmt.setLong(1, event.getOrigin().getId());
			eventInsertStmt.setLong(2, event.getOperation().getId());
			eventInsertStmt.setLong(3, event.getTransactionId());
			eventInsertStmt.setString(4, event.getIdentification());
			eventInsertStmt.setString(5, event.getType());
			eventInsertStmt.setTimestamp(6, event.getPublishTimestamp());
			eventInsertStmt.setTimestamp(7, event.getDequeueTimestamp());
			eventInsertStmt.setTimestamp(8, updateTimestamp);
			eventInsertStmt.setString(9, event.getTrxid());
			eventInsertStmt.setString(10, event.getState());
			eventInsertStmt.setString(11, event.getSource());
			
			eventInsertStmt.executeUpdate();
			/* Now retrieve the generated EventMessage id */
			ResultSet rs=eventInsertStmt.getGeneratedKeys();
			if(rs.next()){
				eventId = rs.getInt(1);
			} else {
				throw new EventMessagePersistenceException("DB Error - EventMessage: ID not generated!" );
			}
		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - EventMessage: " + ex.getMessage(), ex);
		}
		event.setId(eventId);
		event.setUpdateTime(updateTime);
		event.setUpdateTimestamp(updateTimestamp);
	}

	/**
	 * Save the event processing result.
	 * The processing result status and info is updated in the executed event.
	 * The group is also updated with a correct execution (lastExecutedEventMessageId = eventId || null, retryableEventMessageId = null || eventId)
	 * the passed instance of group should be already updated!
	 * 
	 * @param result
	 * @throws EventMessagePersistenceException
	 */
	public void saveProcessingResult(ProcessingResult result, EventCollectorGroup ecg) throws EventMessagePersistenceException {
		LocalDateTime updateTime = LocalDateTime.now();
		Timestamp updateTimestamp = Timestamp.valueOf(updateTime);
		try {
			/*
			   1-EventMessage.state, 2-EventMessage.processingInfo, 3-EventMessage.updatedDate,
			   4-EventCollectorGroup.retryableEventMessageId, 5-EventCollectorGroup.lastExecutedEventMessageId, 6-EventCollectorGroup.updatedTimestamp,
			   7-EventMessage.id, 8-EventCollectorGroup.id
		    */
			eventUpdateStmt.setString(1, result.getState());
			if (result.hasResultInfo())
				eventUpdateStmt.setString(2, result.getResultInfo());
			else
				eventUpdateStmt.setNull(2, java.sql.Types.VARCHAR);
			eventUpdateStmt.setTimestamp(3, updateTimestamp);
			if (ecg.getRetryableEventMessageId() > 0) {
				eventUpdateStmt.setLong(4, ecg.getRetryableEventMessageId());
				eventUpdateStmt.setNull(5, java.sql.Types.BIGINT);
			} else {
				eventUpdateStmt.setNull(4, java.sql.Types.BIGINT);
				eventUpdateStmt.setLong(5, ecg.getLastExecutedEventMessageId());
			}
			eventUpdateStmt.setTimestamp(6, updateTimestamp);
			eventUpdateStmt.setLong(7, result.getEventId());
			eventUpdateStmt.setLong(8, ecg.getId());
			
			eventUpdateStmt.executeUpdate();

		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - Updating EventMessage: " + ex.getMessage(), ex);
		}
	}

	/**
	 * Save the event processing result.
	 * The processing result status and info is updated in the executed event.
	 * 
	 * @param result
	 * @throws EventMessagePersistenceException
	 */
	public void saveProcessingResult(ProcessingResult result) throws EventMessagePersistenceException {
		LocalDateTime updateTime = LocalDateTime.now();
		Timestamp updateTimestamp = Timestamp.valueOf(updateTime);
		try {
			/*
			   1-state, 2-processingInfo, 3-updatedDate, 4-id
		    */
			eventSingleUpdateStmt.setString(1, result.getState());
			if (result.hasResultInfo())
				eventSingleUpdateStmt.setString(2, result.getResultInfo());
			else
				eventSingleUpdateStmt.setNull(2, java.sql.Types.VARCHAR);
			eventSingleUpdateStmt.setTimestamp(3, updateTimestamp);
			eventSingleUpdateStmt.setLong(4, result.getEventId());
			
			eventSingleUpdateStmt.executeUpdate();

		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - Updating EventMessage: " + ex.getMessage(), ex);
		}
	}

	/**
	 * Retrieve the EventCollectorGroup by name.
	 * 
	 * @param _name, name of the EventCollectorGroup to retrieve.
	 * @return EventCollectorGroup, a configured instance matching with given name.
	 * @throws EventMessagePersistenceException, if an error occurs
	 */
	public EventCollectorGroup retrieveEventCollectorGroup(String _name) throws EventMessagePersistenceException {
		long id;
		long retryableEventMessageId;
		long lastExecutedEventMessageId;
		long failedEventsRetryableSeconds;
		Timestamp updatedTimestamp;

		try {
			collectorGroupRetrieveStmt.setString(1, _name);
			
			ResultSet rs = collectorGroupRetrieveStmt.executeQuery();
			/* RETURN:
			 * 	id, retryableEventMessageId, lastExecutedEventMessageId, failedEventsRetryableSeconds, updatedTimestamp */
			if(rs.next()){
				id = rs.getLong(1);
				retryableEventMessageId = rs.getLong(2);
				lastExecutedEventMessageId = rs.getLong(3);
				failedEventsRetryableSeconds = rs.getLong(4);
				updatedTimestamp = rs.getTimestamp(5);
			} else {
				throw new EventMessagePersistenceException("DB Error - EventCollectorGroup: '" + _name + "' not found!" );
			}
		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - EventCollectorGroup: " + ex.getMessage(), ex);
		}
		
		EventCollectorGroup ecg = new EventCollectorGroup(id, _name, retryableEventMessageId, lastExecutedEventMessageId, failedEventsRetryableSeconds, updatedTimestamp);

		// Retrieve eventCollectorGroup targets (from each group origin)
		this.retrieveEventMessageTargets(ecg);
		// Retrieve eventCollectorGroup origins and add them to new eventCollectorGroup
		this.loadEventCollectorGroupOrigins(ecg);

		return ecg;
	}

	/**
	 * Retrieve all eventMessages from DB in 'pending' or 'retryable' state with ID >= anEventMessageId that
	 * belongs to any origin of the ecg.
	 * 
	 * @param anEventMessageId, first included message id to retrieve.
	 * @param ecg, EventCollectorGroup owner of retrieved messages.
	 * @return a List<EventMessage> with all messages of the group ready to process.
	 * @throws EventMessagePersistenceException if an error occurs.
	 */
	public List<EventMessage> retrieveProcessableEventsIncluding(long anEventMessageId, EventCollectorGroup ecg) throws EventMessagePersistenceException {
		return this.retrieveProcessableEvents(anEventMessageId, ecg, db_event_retrieveIncludingQuery);
	}

	/**
	 * Retrieve all eventMessages from DB in 'pending' state with ID > anEventMessageId that
	 * belongs to any origin of the ecg.
	 * 
	 * @param anEventMessageId, nearest minor ID to retrieve messages with highest ID.
	 * @param ecg, EventCollectorGroup owner of retrieved messages.
	 * @return a List<EventMessage> with all messages of the group ready to process.
	 * @throws EventMessagePersistenceException if an error occurs.
	 */
	public List<EventMessage> retrieveProcessableEventsAfter(long anEventMessageId, EventCollectorGroup ecg) throws EventMessagePersistenceException {
		return this.retrieveProcessableEvents(anEventMessageId, ecg, db_event_retrieveAfterQuery);
	}

	/******************************************************************************
	 * 
	 * Retryer persistence interface
	 * 
	 ******************************************************************************/

	/**
	 * Get the initial eventMessageID belonging to group, considering a window period of
	 * group.failedEventsRetryableSeconds.
	 * 
	 * @param group, an EventCollectorGroup owner of target eventMessage to found.
	 * @return a long with eventMessage ID
	 * @throws OnErrorEventMessageNotFound, if no message found
	 * @throws EventMessagePersistenceException, if an error occurs
	 * 
	 */
	public long getInitialOnErrorEventMessageID(EventCollectorGroup group) throws OnErrorEventMessageNotFound, EventMessagePersistenceException {
		PreparedStatement stmt;
		
		try {
			stmt = this.configuredStmtForQueryWithOriginsAndAnotherID(group.getFailedEventsRetryableSeconds(), group.getSortedOriginIDs(), db_eventMessage_onErrorInitialID_query);
			ResultSet rs = stmt.executeQuery();
			if (rs.next()) {
				return rs.getLong(1);
			}
		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - Retrieving on error EventMessageIDs by Element: " + ex.getMessage(), ex);
		}

		throw new OnErrorEventMessageNotFound();
	}

	/**
	 * Retrieve first on error message by element type from group starting at startId.
	 * 
	 * @param group, EventCOllectorGroup which owns messages to analyze.
	 * @param startId, first window period message id, messages analyze should start from him.
	 * @return messagesByElemType map.
	 * @throws EventMessagePersistenceException, if an error occurs
	 */
	public Map<String, EventMessage> getOnErrorMessagesByElement(EventCollectorGroup group, long startId) throws EventMessagePersistenceException {
		Map<String, EventMessage> messages = new HashMap<String, EventMessage>();
		
		PreparedStatement stmt;
		
		try {
			stmt = this.configuredStmtForQueryWithOriginsAndAnotherID(startId, group.getSortedOriginIDs(), db_eventMessage_idsByElement_query);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				long eventId = rs.getLong(2);
				String identification = rs.getString(1);
				eventRetrieveOneStmt.setLong(1, eventId);
				ResultSet ers = eventRetrieveOneStmt.executeQuery();
				if (ers.next()) {
					messages.put(identification, this.retrieveOneEventMessage(ers, group));
				} else {
					throw new EventMessagePersistenceException("DB Error - Retrieving on error EventMessages by Element - Event Message Not Fount ID: " + Long.toString(eventId));					
				}
			}
		} catch (SQLException|EventMessageException ex) {
			throw new EventMessagePersistenceException("DB Error - Retrieving on error EventMessages by Element: " + ex.getMessage(), ex);
		}

		return messages;
	}
	
	/**
	 * Get event near subsequent message id for the same element identifier and origin with successful processing (state IN ['ok', 'warning']).
	 * If there is no successfully message, return null.
	 * 
	 * @param event, an EventMessage from which the search should start.
	 * @param identification, a String, element identifier to which the messages belongs. 
	 * @param group, an EventCollectorGroup which owns messages to analyze.
	 * @return, a Long object with message okID or null if not found.
	 * @throws EventMessagePersistenceException, if an error occurs
	 */
	public Long getSuccessfulyProcessedMessageIDAfter(EventMessage event, String identification, EventCollectorGroup group) throws EventMessagePersistenceException {
		
		try {
			/* 1-id, 2-identification, 3-eventMessageOriginId */
			eventMessageOkMessageIdAfterStmt.setLong(1, event.getId());
			eventMessageOkMessageIdAfterStmt.setString(2, identification);
			eventMessageOkMessageIdAfterStmt.setLong(3, event.getOrigin().getId());

			ResultSet rs = eventMessageOkMessageIdAfterStmt.executeQuery();
			/* RETURN:
			 * 	id */
			if (rs.next()) {
				long id = rs.getLong(1);
				return (rs.wasNull() ? null : Long.valueOf(id));
			}
		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - EventMessage Successfuly processed after: " + ex.getMessage(), ex);
		}

		return null;
	}

	/**
	 * Set as 'obsolet' all on error messages between passed event id and okID that belong to the same event origin
	 * and same element identification.
	 * 
	 * @param identification, a String, element identifier to which the messages to mark as 'obsolet' belongs.
	 * @param event, an EventMessage as the first message to mark as 'obsolet'.
	 * @param okID, a long with the ID of the first posterior message of the same element successfully processed.
	 * @param group, EventCollectorGroup which owns the messages.
	 * @return eventMarkedCount, an int with updated messages as 'obsolet'.
	 * @throws EventMessagePersistenceException, if an error occurs.
	 */
	public int setObsoletOnErrorMessagesOfElement(String identification, EventMessage event, long okID, EventCollectorGroup group) throws EventMessagePersistenceException {
		try {
			/* 1-identification, 2-okID, 3-originalID, 4-eventMessageOriginId */
			eventMessageUpdateAsObsoletStmt.setString(1, identification);
			eventMessageUpdateAsObsoletStmt.setLong(2, okID);
			eventMessageUpdateAsObsoletStmt.setLong(3, event.getId());
			eventMessageUpdateAsObsoletStmt.setLong(4, event.getOrigin().getId());

			return eventMessageUpdateAsObsoletStmt.executeUpdate();
			
		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - EventMessage mark as OBSOLET: " + ex.getMessage(), ex);
		}
	}

	/**
	 * Retrieve all on error messages belonging to the group starting from startId.
	 * 
	 * @param startId, a long with the initial message id to retrieve.
	 * @param group, an EventCollectorGroup which owns the retrieved messages.
	 * @return a message list.
	 * @throws EventMessagePersistenceException, if an error occurs
	 */
	public List<EventMessage> retrieveReProcessableEventsStartingAt(long startId, EventCollectorGroup group) throws EventMessagePersistenceException {
		PreparedStatement stmt;
		List<EventMessage> events = new ArrayList<EventMessage>();
		try {
			stmt = this.configuredStmtForQueryWithOriginsAndAnotherID(startId, group.getSortedOriginIDs(), db_eventMessage_onError_retrieveQuery);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				try {
					events.add(this.retrieveOneEventMessage(rs, group));
				} catch (EventMessageException e) {
					throw new EventMessagePersistenceException("DB Error - Retrieving Re-processable EventMessage: " + e.getMessage(), e);
				}
			}
		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - Retrieving Re-processable EventMessages: " + ex.getMessage(), ex);
		}

		return events;
	}

	/******************************************************************************
	 * 
	 * General Private methods
	 * 
	 ******************************************************************************/

	/**
	 * Complete the query string passed (which use origins & anId), then prepare the statement and finally
	 * configure it with passed parameters.
	 * 
	 * @param anId, first parameter of query passed.
	 * @param originIDs, array of origin IDs to replace the sequence "ORIGIN_IDS" in the passed query.
	 * @param queryString, query string to be configured
	 * @return a PreparedStatement configured ready for execution.
	 * @throws SQLException if an error occurs
	 */
	private PreparedStatement configuredStmtForQueryWithOriginsAndAnotherID(long anId, Long[] originIDs, String queryString) throws SQLException {
		StringBuilder sbSql = new StringBuilder( 64 );

		/* Generate a parameter hole (?) for each id part of ids array */
		for( int i=0; i < originIDs.length; i++ ) {
		  if( i > 0 ) sbSql.append( "," );
		  sbSql.append( " ?" );
		}

		PreparedStatement stmt = db_conn.prepareStatement(queryString.replace("ORIGIN_IDS", sbSql.toString()));

		stmt.setLong(1, anId);
		
		/* Now set the ids values */
		for( int i=0; i < originIDs.length; i++ ) {
			stmt.setLong( i+2, originIDs[ i ] );
		}
		
		return stmt;
	}
	
	/**
	 * Complete the query string passed (which use origins & anId) for retrieving events, then prepare the statement and finally
	 * configure it with passed parameters.
	 * 
	 * @param anId, first parameter of query passed.
	 * @param originIDs, array of origin IDs that events should belongs to.
	 * @param partialQuery, query string to be completed
	 * @return a PreparedStatement configured ready for execution.
	 * @throws SQLException if an error occurs
	 */
	private PreparedStatement configureProcessableEventRetrievingStatement(long anId, Long[] originIDs, String partialQuery) throws SQLException {
		StringBuilder sbSql = new StringBuilder( 1024 );
		sbSql.append(partialQuery);

		/* Generate a parameter hole (?) for each id part of ids array */
		for( int i=0; i < originIDs.length; i++ ) {
		  if( i > 0 ) sbSql.append( "," );
		  sbSql.append( " ?" );
		}
		sbSql.append( " )" );
		if (max_processable_events != null) {
			sbSql.append(" LIMIT ");
			sbSql.append(max_processable_events.longValue());
		}

		PreparedStatement stmt = db_conn.prepareStatement(sbSql.toString());

		stmt.setLong(1, anId);
		
		/* Now set the ids values */
		for( int i=0; i < originIDs.length; i++ ) {
			stmt.setLong( i+2, originIDs[ i ] );
		}
		
		return stmt;
	}
	
	/**
	 * Retrieve all eventMessages from DB in the correct state considering anEventMessageId that
	 * belongs to any origin of the ecg.
	 * 
	 * @param anEventMessageId, first included message id to retrieve.
	 * @param ecg, EventCollectorGroup owner of retrieved messages.
	 * @param retrieveQuery, String containing the retrieving partial query to be use.
	 * @param stmt, the PreparedStatement to use for retrieving.
	 * @return a List<EventMessage> with all messages of the group ready to process.
	 * @throws EventMessagePersistenceException if an error occurs.
	 */
	private List<EventMessage> retrieveProcessableEvents(long anEventMessageId, EventCollectorGroup ecg, String retrieveQuery) throws EventMessagePersistenceException {
		PreparedStatement stmt;
		
		List<EventMessage> events = new ArrayList<EventMessage>();
		try {
			stmt = this.configureProcessableEventRetrievingStatement(anEventMessageId, ecg.getSortedOriginIDs(), retrieveQuery);
			ResultSet rs = stmt.executeQuery();
			while (rs.next()) {
				try {
					events.add(this.retrieveOneEventMessage(rs, ecg));
				} catch (EventMessageException e) {
					throw new EventMessagePersistenceException("DB Error - Retrieving EventMessage: " + e.getMessage(), e);
				}
			}
		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - Retrieving EventMessage: " + ex.getMessage(), ex);
		}

		return events;
	}

	/**
	 * Retrieve all fields of current record in rs, build an EventMessage and return it.
	 * 
	 * @param rs, resultSet positioned in the next EventMessage db record.
	 * @param ecg, the EventCollectorGroup containing event origin. 
	 * @return EventMessage, the loaded event message from rs.
	 * @throws SQLException if an error occurs
	 * @throws EventMessageException 
	 */
	private EventMessage retrieveOneEventMessage(ResultSet rs, EventCollectorGroup ecg) throws SQLException, EventMessageException {
		/* RETURN:
		 * 	id, eventMessageOriginId, eventMessageOperationId, transactionId, identification, type, publishDate,
		 *  dequeuedDate, updatedDate, trxid, state, processingInfo, source */

		long id, eventMessageOriginId, eventMessageOperationId, transactionId;
		String identification, type, trxid, state, processingInfo, source;
		Timestamp publishTimestamp, dequeueTimestamp, updatedTimestamp;

		id = rs.getLong(1);
		eventMessageOriginId = rs.getLong(2);
		eventMessageOperationId = rs.getLong(3);
		transactionId = rs.getLong(4);
		identification = rs.getString(5);
		type =  rs.getString(6);
		publishTimestamp = rs.getTimestamp(7);
		dequeueTimestamp = rs.getTimestamp(8);
		updatedTimestamp = rs.getTimestamp(9);
		trxid =  rs.getString(10);
		state =  rs.getString(11);
		processingInfo =  rs.getString(12);
		source =  rs.getString(13);

		EventMessageOrigin origin = ecg.getOrigin(eventMessageOriginId);
		
		return new EventMessage(origin, id, eventMessageOperationId, transactionId, identification, type,
				publishTimestamp, dequeueTimestamp, updatedTimestamp, trxid, state, processingInfo, source);
	}

	/**
	 * Load all configured properties of owner entity with id: _ownerId for owner type
	 * _ownerType and return as a Map(propertyName, propertyValue).
	 * 
	 * @param _ownerId, id of the properties owner entity. 
	 * @param _ownerType, type of properties owner entity
	 * @return Map<String, String>, with propName -> propValue
	 * @throws EventMessagePersistenceException, if an error occurs.
	 */
	private Map<String, String> retrieveConfiguredPropertiesOf(long _ownerId, String _ownerType) throws EventMessagePersistenceException {
		Map<String, String> properties = new HashMap<String, String>();
		String property, value;
		try {
			eventMessageConfigRetrieveStmt.setLong(1, _ownerId);
			eventMessageConfigRetrieveStmt.setString(2, _ownerType);
			ResultSet rs = eventMessageConfigRetrieveStmt.executeQuery();
			/* RETURN:
			 * 	property, value */
			while (rs.next()) {
				property = rs.getString(1);
				value = rs.getString(2);
				
				properties.put(property, value);
			}
		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - EventMessageConfig: " +
					_ownerType + "(" + Long.toString(_ownerId) + ") " +
					ex.getMessage(), ex);
		}
		return properties;
	}

	/**
	 * Create a new EventMessageOrigin with passed parameters, then retrieve the origin configuration properties and
	 * set the target with eventMessageTargetId from cache.
	 * 
	 * @param id, Id of EventMessageOrigin to load.
	 * @param name, String name of new EventMessageOrigin.
	 * @param eventMessageTargetId, Id of target to set in the new EventMessageOrigin.
	 * @param eventCollectorGroupOrder, order in the group of the new EventMessageOrigin.
	 * @return EventMessageOrigin, a new configured instance.
	 * @throws EventMessagePersistenceException, if an error occurs.
	 */
	private EventMessageOrigin buildEventMessageOrigin(long id, String name, long eventMessageTargetId, int eventCollectorGroupOrder) throws EventMessagePersistenceException {
		EventMessageOrigin origin = new EventMessageOrigin(id, name, eventCollectorGroupOrder, targetsByIdMap.get(Long.valueOf(eventMessageTargetId)));
		Map<String, String> properties = this.retrieveConfiguredPropertiesOf(id, EventMessageOrigin.db_type);

		String store_command, db_driver, queue_url, user, password, consumer;

		store_command = properties.get("store_command");
		db_driver = properties.get("db_driver");
		queue_url = properties.get("queue_url");
		user = properties.get("user");
		password = properties.get("password");
		consumer = properties.get("consumer");

		if (store_command == null || db_driver == null || queue_url == null || user == null || password == null || consumer == null) {
			throw new EventMessagePersistenceException("Config ERROR - EventMessageOrigin - Required property missing");
		}

		origin.setStore_command(store_command);
		origin.setDb_driver(db_driver);
		origin.setQueue_url(queue_url);
		origin.setUser(user);
		origin.setPassword(password);
		origin.setConsumer(consumer);
		
		return origin;
	}

	/**
	 * Retrieve all EventMessageOrigins configured for the EventCollectorGroup and
	 * add them to it.
	 * 
	 * @param ecg
	 * @throws EventMessagePersistenceException
	 */
	private void loadEventCollectorGroupOrigins(EventCollectorGroup ecg) throws EventMessagePersistenceException {
		long id;
		String name;
		long eventMessageTargetId;
		int eventCollectorGroupOrder;
		EventMessageOrigin origin;

		try {
			eventMessageOriginRetrieveStmt.setLong(1, ecg.getId());
			ResultSet rs = eventMessageOriginRetrieveStmt.executeQuery();
			/* RETURN:
			 * 	id, name, eventMessageTargetId, eventCollectorGroupOrder */
			while (rs.next()) {
				id = rs.getLong(1);
				name = rs.getString(2);
				eventMessageTargetId = rs.getLong(3);
				eventCollectorGroupOrder = rs.getInt(4);
				
				/* Build a new EventMessageOrigin with a target.... etc */
				origin = this.buildEventMessageOrigin(id, name, eventMessageTargetId, eventCollectorGroupOrder);
				ecg.addEventMessageOrigin(origin);
			}
		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - EventMessageOrigin of Group: " +
					ecg.getName() + " - " +
					ex.getMessage(), ex);
		}
	}

	/**
	 * Create a new EventMessageTarget, with all configured properties and add to cache.
	 * 
	 * @param id
	 * @param name
	 */
	private void buildEventMessageTarget(long id, String name) throws EventMessagePersistenceException {
		EventMessageTarget target = new EventMessageTarget(id, name);
		EventMessageTargetToken token = null;

		Map<String, String> properties = this.retrieveConfiguredPropertiesOf(id, EventMessageTarget.db_type);

		String timeout, max_retries, endPointURL, user, password, rest_tokenURL;
		timeout = properties.get("timeout");
		max_retries = properties.get("max_retries");
		endPointURL = properties.get("endPointURL");
		user = properties.get("user");
		password = properties.get("password");
		rest_tokenURL = properties.get("rest_tokenURL");

		if (timeout == null || max_retries == null || endPointURL == null || user == null || password == null || rest_tokenURL == null) {
			throw new EventMessagePersistenceException("Config ERROR - EventMessageTarget - Required property missing");
		}

		int timeoutInt = Integer.parseInt(timeout);
		token = EventMessageTarget.getToken(rest_tokenURL);
			
		try {
			if (token == null) {
				token = new EventMessageTargetToken(rest_tokenURL, user, password, timeoutInt);
				EventMessageTarget.addToken(token);
			}
			target.setTimeout(timeoutInt);
			target.setMax_retries(Integer.parseInt(max_retries));
			target.setEndPointURL(endPointURL);
			target.setToken(token);
		} catch (EventMessageTragetException ex) {
			throw new EventMessagePersistenceException("Config ERROR - EventMessageTarget: " + ex.getMessage(), ex);
		}

		targetsByIdMap.put(Long.valueOf(id), target);
	}

	/**
	 * Retrieve all EventMessageTargets and cache them for origin reutilization.
	 * 
	 * @param ecg, EventCollectorGRoup owner of targets to retrieve.
	 * @throws EventMessagePersistenceException, if an error occurs.
	 */
	private void retrieveEventMessageTargets(EventCollectorGroup ecg) throws EventMessagePersistenceException {
		long id;
		String name;
		try {
			eventMessageTargetRetrieveStmt.setLong(1, ecg.getId());
			ResultSet rs = eventMessageTargetRetrieveStmt.executeQuery();
			/* RETURN:
			 * 	id, name */
			while (rs.next()) {
				id = rs.getLong(1);
				name = rs.getString(2);
				
				buildEventMessageTarget(id, name);
			}
		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - EventMessageTarget: " + ex.getMessage(), ex);
		}
	}

	/**
	 * Load all event message operations and create them (auto-registered).
	 * 
	 * @throws EventMessagePersistenceException, if an error occurs
	 */
	private void retrieveEventMessageOperations() throws EventMessagePersistenceException {
		long id;
		String name, operationType;
		try {
			ResultSet rs = eventMessageOperationRetrieveStmt.executeQuery();
			/* RETURN:
			 * 	id, name, operationType */
			while (rs.next()) {
				id = rs.getLong(1);
				name = rs.getString(2);
				operationType = rs.getString(3);
				
				switch (operationType) {
					case "create":
						new CreateOperation(id, name);
						break;
					case "update":
						new UpdateOperation(id, name);
						break;
					case "delete":
						new DeleteOperation(id, name);
						break;
					default:
						throw new EventMessagePersistenceException("DB Error - EventMessageOperation: Unknown operation type: " + operationType);
				}
			}
		} catch (SQLException ex) {
			throw new EventMessagePersistenceException("DB Error - EventMessageTarget: " + ex.getMessage(), ex);
		}
	}

	/**
	 * Create a new instance, initialize the db connection and load global objects.
	 * 
	 */
	private EventCollectorPersistenceManager(String driver, String url, String user, String pass, String max_events) throws Exception {
		db_driver = driver;
		db_url = url;
		db_user = user;
		db_password = pass;
		try {
			max_processable_events = (max_events == null ? null : Long.parseLong(max_events));
		} catch(NumberFormatException e) {
			// Nothing to do, just log the error and continue
			Log.getInstance().error("Error parsing max_processable_events: " + e.getMessage(), e);
		}
		
		try {
			this.initializeConnection();
			this.prepareCommands();
			targetsByIdMap = new HashMap<Long, EventMessageTarget>();
			this.retrieveEventMessageOperations();
		} catch (SQLException ex) {
			try {
				db_conn.close();
			} catch (Exception e) {}
			throw ex;
		}
	}

	private void prepareCommands() throws SQLException {
        eventInsertStmt = db_conn.prepareStatement(db_event_insertQuery, Statement.RETURN_GENERATED_KEYS);
        eventUpdateStmt = db_conn.prepareStatement(db_event_updateQuery);
        eventRetrieveOneStmt = db_conn.prepareStatement(db_event_retrieveOneQuery);
        eventSingleUpdateStmt = db_conn.prepareStatement(db_event_single_updateQuery);
        collectorGroupRetrieveStmt = db_conn.prepareStatement(db_collectorGroup_retrieveQuery);
        eventMessageTargetRetrieveStmt = db_conn.prepareStatement(db_target_retrieveQuery);
        eventMessageOperationRetrieveStmt = db_conn.prepareStatement(db_operation_retrieveQuery);
        eventMessageConfigRetrieveStmt = db_conn.prepareStatement(db_config_retrieveQuery);
        eventMessageOriginRetrieveStmt = db_conn.prepareStatement(db_eventMessageOrigin_retrieveQuery);
        eventMessageOkMessageIdAfterStmt = db_conn.prepareStatement(db_eventMessage_okMessageIdAfter_query);
        eventMessageUpdateAsObsoletStmt = db_conn.prepareStatement(db_eventMessage_updateAsObsolet_query);
	}

	private void initializeConnection() throws Exception {

		DriverManager.registerDriver((Driver)Class.forName(db_driver).newInstance());
			 
		db_conn = DriverManager.getConnection(db_url, db_user, Utils.decrypt(db_password));

		System.out.println("JDBC Connection opened "); 
	}

	/**
	 * Llibera los recursos tomados, realiza un rollback y luego cierra lo conexion.
	 * 
	 */
	private void release() {
		try {
			db_conn.rollback();
			db_conn.close();
		} catch (SQLException ex) {
			/* Nothing relevant to do, just log the error as WARNING. */
			Log.getInstance().warning("Error clossing persistence connection", ex);
		}
	}
}
