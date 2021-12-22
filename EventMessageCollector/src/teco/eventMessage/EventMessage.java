/**
 * 
 */
package teco.eventMessage;

import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

import org.json.JSONException;
import org.json.JSONObject;

import teco.eventMessage.operation.EventMessageOperation;
import teco.eventMessage.origin.EventMessageOrigin;
import teco.eventMessage.persistence.EventCollectorPersistenceManager;
import teco.eventMessage.EventMessageFormatException;

/**
 * @author u190438
 *
 */
public class EventMessage {
	private long id;
	private String identification;
	private String type;
	private LocalDateTime dequeueTime;
	private Timestamp dequeueTimestamp;
	private LocalDateTime publishedTime;
	private Timestamp publishTimestamp;
	private LocalDateTime updateTime;
	private Timestamp updateTimestamp;
	private long transactionId;
	private String trxid;
	private String state = "pending";
	private String processingInfo = null;
	private String source;
	private EventMessageOperation operation;
	private EventMessageOrigin origin;

	private static DateTimeFormatter formatter = DateTimeFormatter.ofPattern("dd-MM-yyyy'T'HH:mm:ss");

	/**
	 * Constructor intended for new message from JSON source.
	 * Do not use for load from internal storage!
	 * 
	 */
	public EventMessage(EventMessageOrigin _origin, String _source, long _transactionId) throws EventMessageException {
		JSONObject jsonMsj;
		JSONObject jsonEventData;
		String operationStr;
		String publishDateStr;
		
		origin = _origin;
		transactionId = _transactionId;
		source = _source;
		
		try {
			jsonMsj = new JSONObject(_source);

			jsonEventData = jsonMsj.getJSONObject("eventData");

			operationStr = jsonEventData.getString("operation");
			type = jsonEventData.getString("type");
			trxid = jsonEventData.getString("trxId");
			identification = jsonEventData.getString("identification");

			publishDateStr = jsonEventData.getString("publishDate");
			
		} catch (JSONException jex) {
			throw new EventMessageFormatException("Format Error - Event Message: " + jex.getMessage(), jex);
		}

		/* The JSON looks correct, proceed to complete the new instance and save it to local db  */
		publishedTime = LocalDateTime.parse(publishDateStr, formatter);
		publishTimestamp = Timestamp.valueOf(publishedTime);
		dequeueTime = LocalDateTime.now();
		dequeueTimestamp = Timestamp.valueOf(dequeueTime);
		operation = EventMessageOperation.getOperation(operationStr);
		
		EventCollectorPersistenceManager.getInstance().saveEvent(this);
	}

	/**
	 * Constructor intended for message loaded from internal storage.
	 * Do not use for new message from JSON source!
	 * 
	 * @param _origin, the EventMessageOrigin owner of event
	 * @param _id
	 * @param _eventMessageOperationId
	 * @param _transactionId
	 * @param _identification
	 * @param _type
	 * @param _publishTimestamp
	 * @param _dequeueTimestamp
	 * @param _updateTimestamp
	 * @param _trxid
	 * @param _state
	 * @param _processingInfo, can be null if was not processed yet
	 * @param _source
	 */
	public EventMessage(EventMessageOrigin _origin, long _id, long _eventMessageOperationId, long _transactionId, 
			String _identification, String _type, Timestamp _publishTimestamp, Timestamp _dequeueTimestamp, Timestamp _updateTimestamp,
			String _trxid, String _state, String _processingInfo, String _source) throws EventMessageException {
		origin = _origin;
		id = _id;
		operation = EventMessageOperation.getOperation(_eventMessageOperationId);
		transactionId = _transactionId;
		identification = _identification;
		type = _type;
		publishTimestamp = _publishTimestamp;
		publishedTime = publishTimestamp.toLocalDateTime();
		dequeueTimestamp = _dequeueTimestamp;
		dequeueTime = dequeueTimestamp.toLocalDateTime();
		updateTimestamp = _updateTimestamp;
		updateTime = updateTimestamp.toLocalDateTime();
		trxid = _trxid;
		state = _state;
		processingInfo = _processingInfo;
		source = _source;
	}

	/**
	 * Describe the event - short version
	 * 
	 * @return String, with a short description of the event.
	 */
	public String getShortDescription() {
		return "<ID: " + Long.toString(id) + " - " +
				"Identification: " + identification + " - " + 
				"Type: " + type + " - " +
				"Operation: " + operation.getOriginalOperation() +">";
	}


	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getIdentification() {
		return identification;
	}

	public void setIdentification(String identification) {
		this.identification = identification;
	}

	public String getType() {
		return type;
	}

	public void setType(String type) {
		this.type = type;
	}

	public LocalDateTime getDequeueTime() {
		return dequeueTime;
	}

	public void setDequeueTime(LocalDateTime dequeueTime) {
		this.dequeueTime = dequeueTime;
	}

	public Timestamp getDequeueTimestamp() {
		return dequeueTimestamp;
	}

	public void setDequeueTimestamp(Timestamp dequeueTimestamp) {
		this.dequeueTimestamp = dequeueTimestamp;
	}

	public LocalDateTime getPublishedTime() {
		return publishedTime;
	}

	public void setPublishedTime(LocalDateTime publishedTime) {
		this.publishedTime = publishedTime;
	}

	public Timestamp getPublishTimestamp() {
		return publishTimestamp;
	}

	public void setPublishTimestamp(Timestamp publishTimestamp) {
		this.publishTimestamp = publishTimestamp;
	}

	public LocalDateTime getUpdateTime() {
		return updateTime;
	}

	public void setUpdateTime(LocalDateTime updateTime) {
		this.updateTime = updateTime;
	}

	public Timestamp getUpdateTimestamp() {
		return updateTimestamp;
	}

	public void setUpdateTimestamp(Timestamp updateTimestamp) {
		this.updateTimestamp = updateTimestamp;
	}

	public long getTransactionId() {
		return transactionId;
	}

	public void setTransactionId(long transactionId) {
		this.transactionId = transactionId;
	}

	public String getTrxid() {
		return trxid;
	}

	public void setTrxid(String trxid) {
		this.trxid = trxid;
	}

	public String getState() {
		return state;
	}

	public void setState(String state) {
		this.state = state;
	}

	public String getProcessingInfo() {
		return processingInfo;
	}

	public void setProcessingInfo(String processingInfo) {
		this.processingInfo = processingInfo;
	}

	public String getSource() {
		return source;
	}

	public void setSource(String source) {
		this.source = source;
	}

	public EventMessageOperation getOperation() {
		return operation;
	}

	public void setOperation(EventMessageOperation operation) {
		this.operation = operation;
	}

	public EventMessageOrigin getOrigin() {
		return origin;
	}

	public void setOrigin(EventMessageOrigin origin) {
		this.origin = origin;
	}

}
