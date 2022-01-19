/**
 * 
 */
package teco.eventMessage.collector;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import teco.eventMessage.EventMessage;
import teco.eventMessage.origin.EventMessageOrigin;
import teco.eventMessage.persistence.EventCollectorPersistenceManager;
import teco.eventMessage.persistence.EventMessagePersistenceException;
import teco.eventMessage.retryer.OnErrorEventMessageNotFound;

/**
 * @author u190438
 *
 */
public class EventCollectorGroup {

	private long id;
	private String name;
	private long retryableEventMessageId;
	private long lastExecutedEventMessageId;
	private long failedEventsRetryableSeconds;
	private LocalDateTime updatedTime;
	private Timestamp updatedTimestamp;

	private List<EventMessageOrigin> originList;
	private Map<Long, EventMessageOrigin> originMap;
	private boolean isOriginListSorted = false;

	/**
	 * Basic instance creation used only by EventCollectorPersistenceManager.
	 * 
	 */
	public EventCollectorGroup(long _id, String _name, long _retryableEventMessageId, long _lastExecutedEventMessageId, long _failedEventsRetryableSeconds, Timestamp _updatedTimestamp) {
		id = _id;
		name = _name;
		retryableEventMessageId = _retryableEventMessageId;
		lastExecutedEventMessageId = _lastExecutedEventMessageId;
		failedEventsRetryableSeconds = _failedEventsRetryableSeconds;
		updatedTimestamp = _updatedTimestamp;
		updatedTime = updatedTimestamp.toLocalDateTime();
		originList = new ArrayList<EventMessageOrigin>();
		originMap = new HashMap<Long, EventMessageOrigin>();
	}

	/**
	 * Collect all ready to process messages from local storage.
	 * 
	 *  If have retryableEventMessage stored, then it and all 'pending' events after will be returned,
	 *  	else all 'pending' events after lastExecutedEventMessage (could be 0) will be returned.
	 *  If no previous execution stored (the first time), both ids will be 0.
	 * 
	 * @return List<EventMessage> with events to process in order. 
	 * @throws EventMessagePersistenceException if an error occurs.
	 */
	public List<EventMessage> getProcessableEvents() throws EventMessagePersistenceException {
		if (retryableEventMessageId > 0)
			return EventCollectorPersistenceManager.getInstance().retrieveProcessableEventsIncluding(retryableEventMessageId, this);

		return EventCollectorPersistenceManager.getInstance().retrieveProcessableEventsAfter(lastExecutedEventMessageId, this);
	}

	/********************************************************************************
	 * 
	 * Retryer interface
	 * 
	 ********************************************************************************/
	
	/**
	 * Get the initial on error eventMessageID belonging to the group, considering the window period of
	 * failedEventsRetryableSeconds.
	 * 
	 * @return, a long id of the first message of the window period.
	 * @throws OnErrorEventMessageNotFound, if no message with state='error' found.
	 */
	public long getInitialOnErrorEventMessageID() throws OnErrorEventMessageNotFound {
		return EventCollectorPersistenceManager.getInstance().getInitialOnErrorEventMessageID(this);
	}
	
	/**
	 * Retrieve first on error message ID by element type from group starting at startId.
	 * 
	 * @param startId, first window period message id, messages analyze should start from him.
	 * @return messageIDByElemType map
	 * @throws EventMessagePersistenceException, if an error occurs
	 */
	public Map<String, Long> getOnErrorMessageIDsByElement(long startId) throws EventMessagePersistenceException {
		return EventCollectorPersistenceManager.getInstance().getOnErrorMessageIDsByElement(this, startId);
	}
	
	/**
	 * @param startId
	 * @param identification
	 * @return
	 * @throws EventMessagePersistenceException
	 */
	public Long getSuccessfulyProcessedMessageIDAfter(long startId, String identification) throws EventMessagePersistenceException {
		return EventCollectorPersistenceManager.getInstance().getSuccessfulyProcessedMessageIDAfter(startId, identification, this);
	}

	/**
	 * @param identification
	 * @param startId
	 * @param okID
	 * @return
	 * @throws EventMessagePersistenceException
	 */
	public int setObsoletOnErrorMessagesOfElement(String identification, long startId, long okID) throws EventMessagePersistenceException {
		return EventCollectorPersistenceManager.getInstance().setObsoletOnErrorMessagesOfElement(identification, startId, okID, this);
	}

	/**
	 * @param startId
	 * @return
	 * @throws EventMessagePersistenceException
	 */
	public List<EventMessage> getReProcessableEvents(long startId) throws EventMessagePersistenceException {
		return EventCollectorPersistenceManager.getInstance().retrieveReProcessableEventsStartingAt(startId, this);
	}

	/********************************************************************************
	 * 
	 * Accessors
	 * 
	 ********************************************************************************/

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getName() {
		return name;
	}

	public void setName(String name) {
		this.name = name;
	}

	public long getRetryableEventMessageId() {
		return retryableEventMessageId;
	}

	public void setRetryableEventMessageId(long retryableEventMessageId) {
		this.retryableEventMessageId = retryableEventMessageId;
	}

	public long getLastExecutedEventMessageId() {
		return lastExecutedEventMessageId;
	}

	public void setLastExecutedEventMessageId(long lastExecutedEventMessageId) {
		this.lastExecutedEventMessageId = lastExecutedEventMessageId;
	}

	public long getFailedEventsRetryableSeconds() {
		return failedEventsRetryableSeconds;
	}

	public void setFailedEventsRetryableSeconds(long failedEventsRetryableSeconds) {
		this.failedEventsRetryableSeconds = failedEventsRetryableSeconds;
	}

	public LocalDateTime getUpdatedTime() {
		return updatedTime;
	}

	public void setUpdatedTime(LocalDateTime updatedTime) {
		this.updatedTime = updatedTime;
	}

	public Timestamp getUpdatedTimestamp() {
		return updatedTimestamp;
	}

	public void setUpdatedTimestamp(Timestamp updatedTimestamp) {
		this.updatedTimestamp = updatedTimestamp;
	}

	/**
	 * Add passed EventMessageOrigin to end of orginList and register it in originMap.
	 * 
	 * @param origin
	 */
	public void addEventMessageOrigin(EventMessageOrigin origin) {
		originList.add(origin);
		originMap.put(Long.valueOf(origin.getId()), origin);
		origin.setGroup(this);
	}
	
	/**
	 * Return the EventMessageOrigin with id = passed ID.
	 * 
	 * @param eventMessageOriginId, id of origin requested.
	 * @return the EventMessageOrigin.
	 */
	public EventMessageOrigin getOrigin(long eventMessageOriginId) {
		return originMap.get(Long.valueOf(eventMessageOriginId));
	}

	/**
	 * @return the List<EventMessageOrigin> sorted.
	 *  
	 */
	public List<EventMessageOrigin> getSortedOriginList() {
		if (!isOriginListSorted) {
			Collections.sort(originList, (o1, o2) -> o1.getEventCollectorGroupOrder() - o2.getEventCollectorGroupOrder());
		}
		return originList;
	}
	
	/**
	 * Collect all origin ids.
	 * 
	 * @return Long[] array with origin IDs.
	 */
	public Long[] getSortedOriginIDs() {
		int lenght = this.getSortedOriginList().size();
		Long[] ids = new Long[lenght];
		for (int i = 0; i < lenght; i++) {
			ids[i] = Long.valueOf(originList.get(i).getId());
		}
		return ids;
	}
}
