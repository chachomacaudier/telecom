/**
 * Persistence management for hole application.
 * 
 */
package teco.eventMessage.persistence;

import java.sql.Array;
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
			+ "SET EventMessage.state=?, EventMessage.processingInfo=?, EventMessage.updatedDate=? "
			+ "SET EventCollectorGroup.retryableEventMessageId=?, EventCollectorGroup.lastExecutedEventMessageId=? "
			+ "WHERE EventMessage.id=? AND EventCollectorGroup.id=?;";
	static private String db_event_retrieveIncludingQuery = "SELECT id, eventMessageOriginId, eventMessageOperationId, transactionId, identification, type, publishDate, "
			+ "dequeueDate, updateDate, trxid, state, processingInfo, source "
			+ "FROM `EventMessage` WHERE id >= ? AND state IN ('pending', 'retriable') AND eventMessageOriginId IN (?);";
	static private String db_event_retrieveAfterQuery = "SELECT id, eventMessageOriginId, eventMessageOperationId, transactionId, identification, type, publishDate, " 
			+ "dequeueDate, updateDate, trxid, state, processingInfo, source " 
			+ "FROM `EventMessage` WHERE id > ? AND state = 'pending' AND eventMessageOriginId IN (?);";
	
	 
	/* EventCollectoGroup queries */
	static private String db_collectorGroup_retrieveQuery = "SELECT id, retryableEventMessageId, lastExecutedEventMessageId, failedEventsRetryableSeconds, updatedTimestamp "
			+ "FROM EventCollectorGroup WHERE name = ? limit 1;";
	static private String db_collectorGroup_updateQuery = "UPDATE EventCollectorGroup SET retryableEventMessageId=?, lastExecutedEventMessageId=?, updatedTimestamp=? WHERE id=?;";

	/* EventMessageTarget queries */
	static private String db_target_retrieveQuery = "SELECT id, name FROM EventMessageTarget;";

	/* EventMessageOperation queries */
	static private String db_operation_retrieveQuery = "SELECT id, name, operationType FROM EventMessageOperation;";

	/* EventMessageConfig queries */
	static private String db_config_retrieveQuery = "SELECT property, value FROM EventMessageConfig WHERE ownerId = ? AND ownerType = ?;";

	/* EventMessageOrigin queries */
	static private String db_eventMessageOrigin_retrieveQuery = "SELECT id, name, eventMessageTargetId, eventCollectorGroupOrder "
			+ "FROM EventMessageOrigin WHERE eventCollectorGroupId = ?;";

	private PreparedStatement eventInsertStmt;
	private PreparedStatement eventUpdateStmt;
	private PreparedStatement eventRetrieveIncludingStmt;
	private PreparedStatement eventRetrieveAfterStmt;
	private PreparedStatement collectorGroupRetrieveStmt;
	private PreparedStatement collectorGroupUpdateStmt;
	private PreparedStatement eventMessageTargetRetrieveStmt;
	private PreparedStatement eventMessageOperationRetrieveStmt;
	private PreparedStatement eventMessageConfigRetrieveStmt;
	private PreparedStatement eventMessageOriginRetrieveStmt;

	private String db_driver;
	private String db_url;
	private String db_user;
	private String db_password;
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
		
		if (driver == null || url == null || user == null || password == null)
			throw new EventMessageConfigurationException("Required configuration property missing!");

		releaseInstance();
		instance = new EventCollectorPersistenceManager(driver, url, user, password);
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
			   4-EventCollectorGroup.retryableEventMessageId, 5-EventCollectorGroup.lastExecutedEventMessageId,
			   6-EventMessage.id, 7-EventCollectorGroup.id
		    */
			eventUpdateStmt.setString(1, result.getState());
			if (result.hasResultInfo())
				eventUpdateStmt.setString(2, result.getResultInfo());
			else
				eventUpdateStmt.setNull(2, java.sql.Types.VARCHAR);
			eventUpdateStmt.setTimestamp(3, updateTimestamp);
			if (ecg.getLastExecutedEventMessageId() > 0) {
				eventUpdateStmt.setLong(4, ecg.getLastExecutedEventMessageId());
				eventUpdateStmt.setNull(5, java.sql.Types.BIGINT);
			} else {
				eventUpdateStmt.setNull(4, java.sql.Types.BIGINT);
				eventUpdateStmt.setLong(5, ecg.getRetryableEventMessageId());
			}
			eventUpdateStmt.setLong(6, result.getEventId());
			eventUpdateStmt.setLong(7, ecg.getId());
			
			eventUpdateStmt.executeUpdate();

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
		return this.retrieveProcessableEvents(anEventMessageId, ecg, eventRetrieveIncludingStmt);
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
		return this.retrieveProcessableEvents(anEventMessageId, ecg, eventRetrieveAfterStmt);
	}

	/******************************************************************************
	 * 
	 * General Private methods
	 * 
	 ******************************************************************************/

	/**
	 * Retrieve all eventMessages from DB in the correct state considering anEventMessageId that
	 * belongs to any origin of the ecg.
	 * 
	 * @param anEventMessageId, first included message id to retrieve.
	 * @param ecg, EventCollectorGroup owner of retrieved messages.
	 * @param stmt, the PreparedStatement to use for retrieving.
	 * @return a List<EventMessage> with all messages of the group ready to process.
	 * @throws EventMessagePersistenceException if an error occurs.
	 */
	public List<EventMessage> retrieveProcessableEvents(long anEventMessageId, EventCollectorGroup ecg, PreparedStatement stmt) throws EventMessagePersistenceException {
		Array originIDs;
		try {
			originIDs = db_conn.createArrayOf("VARCHAR", ecg.getSortedOriginIDs());
		} catch (SQLException e) {
			throw new EventMessagePersistenceException("DB Error - Retrieving EventMessage: " + e.getMessage(), e);
		}
		
		List<EventMessage> events = new ArrayList<EventMessage>();
		try {
			stmt.setLong(1, anEventMessageId);
			stmt.setArray(2, originIDs);
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
		 *  dequeueDate, updateDate, trxid, state, processingInfo, source */

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
	 * @throws EventMessagePersistenceException, if an error occurs.
	 */
	private void retrieveEventMessageTargets() throws EventMessagePersistenceException {
		long id;
		String name;
		try {
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
	private EventCollectorPersistenceManager(String driver, String url, String user, String pass) throws Exception {
		db_driver = driver;
		db_url = url;
		db_user = user;
		db_password = pass;
		
		try {
			this.initializeConnection();
			this.prepareCommands();
			targetsByIdMap = new HashMap<Long, EventMessageTarget>();
			this.retrieveEventMessageOperations();
			this.retrieveEventMessageTargets();
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
        eventRetrieveIncludingStmt = db_conn.prepareStatement(db_event_retrieveIncludingQuery);
        eventRetrieveAfterStmt = db_conn.prepareStatement(db_event_retrieveAfterQuery);
        collectorGroupRetrieveStmt = db_conn.prepareStatement(db_collectorGroup_retrieveQuery);
        collectorGroupUpdateStmt = db_conn.prepareStatement(db_collectorGroup_updateQuery);
        eventMessageTargetRetrieveStmt = db_conn.prepareStatement(db_target_retrieveQuery);
        eventMessageOperationRetrieveStmt = db_conn.prepareStatement(db_operation_retrieveQuery);
        eventMessageConfigRetrieveStmt = db_conn.prepareStatement(db_config_retrieveQuery);
        eventMessageOriginRetrieveStmt = db_conn.prepareStatement(db_eventMessageOrigin_retrieveQuery);
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
