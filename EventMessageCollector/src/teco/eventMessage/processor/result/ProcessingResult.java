/**
 * 
 */
package teco.eventMessage.processor.result;

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
	 * Create a new result of executing passed event.
	 *  
	 */
	public ProcessingResult(EventMessage event) {
		this.event = event;
		// TODO Auto-generated constructor stub
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
	 * Specific result subclass should contain the correct execution result info.
	 * No result info by default (null).
	 * 
	 * @return String, with the execution result info.
	 * 
	 */
	public String getResultInfo() {
		return resultInfo;
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
		return true;
	}

	/**
	 * Write a generic representation of this execution result to log with INFO level.
	 * 
	 */
	public void log() {
		Log.getInstance().eventLog("Event executed: " + event.getShortDescription() + " - State: " + this.getState(), Level.INFO);
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
}
