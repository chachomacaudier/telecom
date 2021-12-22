/**
 * 
 */
package teco.eventMessage.operation;

/**
 * @author u190438
 *
 */
public class UpdateOperation extends EventMessageOperation {

	public UpdateOperation(long _id, String operation) {
		super(_id, operation);
	}

	@Override
	public String getRESTMethod() {
		return "POST";
	}
}
