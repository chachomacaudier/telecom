package teco.eventMessage.collector.retriever;

import java.sql.CallableStatement;
import java.sql.Clob;
import java.sql.Connection;
import java.sql.Driver;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Types;

import teco.eventMessage.origin.EventMessageOrigin;
import teco.eventMessage.EventMessage;
import teco.eventMessage.EventMessageFormatException;
import teco.eventMessage.Log;
import teco.eventMessage.collector.retriever.EventRetrieverException;
import teco.eventMessage.Utils;

/**
 * @author u190438
 * 
 * Responsive of retrieve messages from origin events queue.
 * For each new message (in JSON format), create an EventMEssage, store it in DB and only then,
 * confirm the extraction (commits the removing from origin queue).
 *
 * If bad formed event is found, logs an error and discard it.
 *
 */
public class EventRetriever {
	private EventMessageOrigin origin;
	private Connection db_conn;
	private CallableStatement cstmt;
	private long transactionId = System.currentTimeMillis();
	private int eventMessagesCount = 0;

	public EventRetriever(EventMessageOrigin _origin) throws Exception {
		origin = _origin;
		try {
			this.initializeConnection();
			this.prepareCommand();
		} catch (SQLException ex) {
			try {
				db_conn.close();
			} catch (Exception e) {}
			throw ex;
		}
	}

	public int getEventMessagesCount() {
		return eventMessagesCount;
	}

	/**
	 * Retrieve all available events from queue {@origin}.
	 * 
	 * If an error occurs, throws an Exception.
	 * 
	 * @return event retrieved count
	 * @throws EventRetrieverException
	 */
	public int retrieveEvents() throws Exception {
		Log.getInstance().info("Start dequeue events for origin: " + origin.getName() + " - transactionID: " + Long.toString(transactionId));
		try {
			String msj;
			boolean finish = false;
			do {
				msj = this.getNextEvenMessage();
				if (msj == null) {
					finish = true;
				} else {
					/* A new message was retrieved */
					eventMessagesCount++;
					try {
						/* BUILD AND SAVE EVENT */
						new EventMessage(origin, msj, transactionId);
					} catch (EventMessageFormatException emfex) {
						/* The message is not valid format, log error and continue */
						Log.getInstance().warning(emfex.getMessage() + "EVENT_MESSAGE: " + msj, emfex);
					}
					this.messageReceived();
				}

			} while (!finish);
			
		} catch (Exception ex) {
			this.messageMissed();
			Log.getInstance().error("Finish dequeue for origin: " + origin.getName() + " (EVENTS count: " + Integer.toString(this.eventMessagesCount) + ") with ERROR: 	" + ex.getMessage(), ex);
			throw ex;
		}
		Log.getInstance().info("Finish dequeue for origin: " + origin.getName() + " (EVENTS count: " + Integer.toString(this.eventMessagesCount) + ") OK - transactionID: " + Long.toString(transactionId));
		this.release();

		return eventMessagesCount;
	}

	/******************************************************************************
	 * 
	 * General Private methods
	 * 
	 ******************************************************************************/

	/**
	 * Return the message content of next message on top of origin queue.
	 * If no message available return null.
	 * 
	 * IMPORTANT: The message extraction is not confirmed (committed) at this time. The sender should
	 * request the confirmation explicitly (send messageReceived()) when the event was correctly ensured.
	 * Only then the message remove will be confirmed.
	 * 
	 * @return, a String with dequeue message.
	 * @throws EventMessageRetrieverException, a connection error occurs.
	 * 
	 */
	private String getNextEvenMessage() throws EventRetrieverException {
	
		try {
	        cstmt.setString(1, origin.getConsumer());
	        cstmt.execute();
	
	        Clob msj = cstmt.getClob(2);
	
			if (msj != null) 
				return msj.getSubString(1, (int) msj.length());
			else
				return null;
		} catch (SQLException ex) {
			throw new EventRetrieverException("Error retrieving eventMessages: " + ex.getMessage(), ex);
		}
	}

	private void initializeConnection() throws Exception {

		DriverManager.registerDriver((Driver)Class.forName(origin.getDb_driver()).newInstance());
			 
		db_conn = DriverManager.getConnection(origin.getQueue_url(), origin.getUser(), Utils.decrypt(origin.getPassword()));

		System.out.println("JDBC Connection opened "); 
		db_conn.setAutoCommit(false);
	}
	
	private void prepareCommand() throws SQLException {
        cstmt = db_conn.prepareCall(origin.getStore_command());
        cstmt.registerOutParameter(2, Types.CLOB);
	}

	/**
	 * Confirm that the extracted message is a valid Event and was already saved.
	 * Commit the transaction for last message extraction completion.
	 * 
	 * @throws Exception, if an error occurs
	 */
	private void messageReceived() throws Exception {
		db_conn.commit();
	}

	/**
	 * The last dequeued message was not ensured.
	 * Abort the transaction for last message extraction rollback.
	 * 
	 * @throws Exception, if an error occurs
	 */
	private void messageMissed() throws Exception {
		db_conn.rollback();
	}
	
	/**
	 * Free all resources and close the connection.
	 * 
	 */
	private void release() {
		try {
			db_conn.rollback();
			db_conn.close();
		} catch (SQLException ex) {
			// eh... nada par hacer, por ahi loguear algo...
		}
	}
}
