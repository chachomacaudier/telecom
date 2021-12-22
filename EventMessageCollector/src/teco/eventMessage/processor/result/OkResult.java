/**
 * 
 */
package teco.eventMessage.processor.result;

import teco.eventMessage.EventMessage;

/**
 * @author u190438
 *
 */
public class OkResult extends ProcessingResult {

	public OkResult(EventMessage event) {
		super(event);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getState() {
		return "ok";
	}

}
