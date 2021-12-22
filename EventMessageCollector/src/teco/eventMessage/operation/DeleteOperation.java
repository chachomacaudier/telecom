/**
 * 
 */
package teco.eventMessage.operation;

/**
 * @author u190438
 *
 */
public class DeleteOperation extends EventMessageOperation {

	public DeleteOperation(long _id, String operation) {
		super(_id, operation);
	}

	@Override
	public String getRESTMethod() {
		return "DELETE";
	}
}
