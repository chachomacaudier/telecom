/**
 * 
 */
package teco.eventMessage.processor.result;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.logging.Level;

import teco.eventMessage.EventMessage;
import teco.eventMessage.Log;
import teco.eventMessage.collector.EventCollectorGroup;
import teco.eventMessage.persistence.EventCollectorPersistenceManager;
import teco.eventMessage.persistence.EventMessagePersistenceException;

/**
 * @author u190438
 *
 */
public abstract class ProcessingResult {
	protected EventMessage event;
	protected String resultInfo = null;

	/**
	 * Create a specific ProcessingResult subclass according to passed response code.
	 * - 200: WarningResult
	 * - 204: OkResult
	 * - 422: DiscardedResult
	 * - 404, 409, 5xx: BusinessErrorResult
	 * - 401: RetryableErrorResult
	 * 
	 * Each subclass is responsible for read and parse body response (if there is one expected).
	 * 
	 * @param responseCode, a positive integer value 
	 * @param conn, HttpConnection used for execute the REST Service needed for body response reading.
	 * @param event, original executed MessageEvent.
	 * @return a new ProcessingResult
	 * @throws IOException, if an error occurs during response reading.
	 */
	public static ProcessingResult fromResponse(int responseCode, HttpURLConnection conn, EventMessage event) throws IOException {
		ProcessingResult result = null;

		switch (responseCode) {
		case 204:
			result = new OkResult(event);
			break;
		case 200:
			result = new WarningResult(event);
			break;
		case 422:
			result = new DiscardedResult(event);
			break;
		case 401:
			result = new RetryableErrorResult(event);
			break;
		case 404:
		case 409:
		case 500:
			result = new BusinessErrorResult(event);
			break;
		}

		result.retrieveResultInfo(conn);

		return result;
	}

	/**
	 * Create a new RetryableErrorResult configured with the passed message and originator event. 
	 * 
	 * @param exceptionMessage, message description of result.
	 * @param event, original executed EventMessage
	 * @return a new instance of BusinessErrorMessage
	 */
	public static ProcessingResult resultFromException(String exceptionMessage, EventMessage event) {
		RetryableErrorResult result = new RetryableErrorResult(event);
		result.setResultInfo(exceptionMessage);

		return result; 
	}

	/**
	 * Create a new result of executing passed event.
	 *  
	 */
	public ProcessingResult(EventMessage event) {
		this.event = event;
	}

	
	public long getEventId() {
		return event.getId();
	}

	/**
	 * Return the event execution state represented by the specific class.
	 * Subclass responsibility.
	 * 
	 * @return String, specific result state
	 */
	public abstract String getState();

	/**
	 * Specific subclass should read response body from conn, parse and set redultInfo if is needed.
	 * By default nothing to read.
	 * 
	 * @param conn
	 * @throws IOException
	 */
	protected void retrieveResultInfo(HttpURLConnection conn) throws IOException {
	}

	/**
	 * @return true if resultInfo has content, false otherwise.
	 */
	public boolean hasResultInfo() {
		return resultInfo != null;
	}

	/**
	 * Specific result subclass should contain the correct execution result info.
	 * No result info by default (null).
	 * 
	 * @return String, with the execution result info.
	 * 
	 */
	public String getResultInfo() {
		return resultInfo;
	}

	public void setResultInfo(String resultInfo) {
		this.resultInfo = resultInfo;
	}


	/**
	 * Indicate if the processing of subsequent messages should continue after this result
	 * was returned from last executed event.
	 * 
	 * Only RetryableErrorResult will deny continuation.
	 * 
	 * Default: all events allow continuation.
	 * 
	 * @return boolean
	 */
	public boolean shouldAbortProcessing() {
		return false;
	}

	/**
	 * Build a String with all event related information of interest for logging post processing.
	 * Expected structure: "origin|elem_type|elem_id|source"
	 * 
	 * @return String, with event formated information
	 */
	public String eventInfoForLog() {
		return event.getOrigin().getName() + "|" +
				event.getType() + "|" +
				event.getIdentification() + "|" +
				event.getSource().replaceAll(System.lineSeparator(), "");
	}
	
	/**
	 * Write a generic representation of this execution result to log with INFO level.
	 * 
	 * Anyway, the structure will be kept:
	 * description|origin|elem_type|elem_id|source
	 * 
	 */
	public void log() {
		Log.getInstance().eventLog("Event executed: " + event.getShortDescription() + " - State: " + this.getState() + "|" + this.eventInfoForLog(), Level.INFO);
	}

	/**
	 * Update passed group execution references (lastExecutedEventMessageId, retryableEventMessageId).
	 * Save the event execution result to local store.
	 * 
	 * @param group, an EventCollectorGroup owner of executed event.
	 * @throws EventMessagePersistenceException, if occurs a persistence error.
	 */
	public void save(EventCollectorGroup group) throws EventMessagePersistenceException {
		group.setRetryableEventMessageId(this.shouldAbortProcessing() ? this.getEventId() : 0);
		group.setLastExecutedEventMessageId(this.shouldAbortProcessing() ? 0 : this.getEventId());
		EventCollectorPersistenceManager.getInstance().saveProcessingResult(this, group);
	}

	/**
	 * Save the event execution result to local store.
	 * 
	 * @throws EventMessagePersistenceException, if occurs a persistence error.
	 */
	public void save() throws EventMessagePersistenceException {
		EventCollectorPersistenceManager.getInstance().saveProcessingResult(this);
	}
}
