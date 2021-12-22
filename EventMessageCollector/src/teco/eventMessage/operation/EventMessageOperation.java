/**
 * @author u190438
 * 
 * Represents operation to be executed on target (Update, Create, Delete) for each different alias
 * given by origin ('created','modified','deleted','allocate','deallocate', etc) -> originalOperation.
 * 
 * There is only one instance for each originalOperation, example:
 * 	- for 'allocated' exists one CreateOperation,
 * 	- for 'created' also exists just one CreateOperation, etc.
 * 
 */
package teco.eventMessage.operation;

import java.util.HashMap;
import java.util.Map;

public abstract class EventMessageOperation {
	
	private long id;
	private String originalOperation = "unknown";
	private static Map<String, EventMessageOperation> operationByNameMap = new HashMap<String, EventMessageOperation>();
	private static Map<Long, EventMessageOperation> operationByIdMap = new HashMap<Long, EventMessageOperation>();

	/**
	 * Create a new instance based on originalOperation, and register it in operation maps.
	 * 
	 */
	public EventMessageOperation(long _id, String operation) {
		originalOperation = operation;
		id = _id;
		operationByNameMap.put(operation, this);
		operationByIdMap.put(Long.valueOf(id),this);
	}

	static public EventMessageOperation getOperation(String operation) throws UnknownEventMessageOperationException {
		EventMessageOperation op = operationByNameMap.get(operation);
		if (op == null)
			throw new UnknownEventMessageOperationException("Unknown operation: " + operation);

		return op;
	}

	static public EventMessageOperation getOperation(long _id) throws UnknownEventMessageOperationException {
		EventMessageOperation op = operationByIdMap.get(Long.valueOf(_id));
		if (op == null)
			throw new UnknownEventMessageOperationException("Unknown operation ID: " + Long.toString(_id));

		return op;
	}

	public long getId() {
		return id;
	}

	public void setId(long id) {
		this.id = id;
	}

	public String getOriginalOperation() {
		return originalOperation;
	}
	
	/**
	 * Answer the REST method that should be configured when invoke the target service.
	 * 
	 * @return a String, 'POST', 'DELETE', etc
	 */
	public abstract String getRESTMethod();
}
