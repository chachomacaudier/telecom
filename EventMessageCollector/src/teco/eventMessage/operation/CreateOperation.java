/**
 * 
 */
package teco.eventMessage.operation;

/**
 * @author u190438
 *
 */
public class CreateOperation extends EventMessageOperation {

	public CreateOperation(long _id, String operation) {
		super(_id, operation);
	}

	@Override
	public String getRESTMethod() {
		return "POST";
	}


}
