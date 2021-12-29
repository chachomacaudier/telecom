/**
 * Event processing functionality 
 */
package teco.eventMessage.processor;

import java.util.List;

import teco.eventMessage.EventMessage;
import teco.eventMessage.Log;
import teco.eventMessage.collector.EventCollectorGroup;
import teco.eventMessage.origin.EventMessageOrigin;
import teco.eventMessage.persistence.EventMessagePersistenceException;
import teco.eventMessage.processor.result.ProcessingResult;

/**
 * @author u190438
 *
 */
public class EventProcessor {

	/**
	 * EventCollectorGroup which is responsive for retrieve processable events and remember
	 * last execution (failed or success).
	 */
	private EventCollectorGroup group;

	/**
	 * Basic constructor
	 */
	public EventProcessor(EventCollectorGroup group) {
		this.group = group;
	}

	/**
	 * Execute all processable events from the EventCollectorGroup.
	 * Execution result status is updated for each executed event.
	 * The group is updated with failed executed event (retryable) and will kept last correct
	 * executed event.
	 * 
	 * If all processable events executes correctly ends silently. If a non business error occurs,
	 * an exception will be throw.  
	 * 
	 * @return the events processed count
	 * @throws EventMessageProcessingException if a non business error occurs.
	 */
	public int executeProcessableEvents() throws EventMessageProcessingException {
		List<EventMessage> events = null;
		
		try {
			events = group.getProcessableEvents();
			Log.getInstance().info("(TOTAL EVENTS to process count: " + Integer.toString(events.size()) + ")");
		} catch (EventMessagePersistenceException ex) {
			/* A persistence error occurs retrieving stored events, nothing to do... throw an error */
			throw new EventMessageProcessingException("EventProcessor - Retrieving events: " + ex.getMessage(), ex);
		}
		
		/* Now start processing */
		EventMessageOrigin origin = null;
		ProcessingResult result = null;
		int eventCount = 0;
		for (EventMessage event : events) {
			origin = event.getOrigin();
			result = origin.getTarget().executeEventMessage(event);
			try {
				result.save(group);
			} catch (EventMessagePersistenceException ex) {
				throw new EventMessageProcessingException("EventProcessor - processed events: [" + Integer.toString(eventCount) + "] - Processing event: " + event.getShortDescription() + " - " + ex.getMessage(), ex);
			}
			result.log();
			if (result.shouldAbortProcessing())
				throw new EventMessageProcessingException("EventProcessor: " + result.getResultInfo() + " - processed events: [" + Integer.toString(eventCount) + "] - Processing event: " + event.getShortDescription());
			eventCount++;
		}
		return eventCount;
	}
}
