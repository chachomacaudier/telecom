/**
 * 
 */
package teco.eventMessage.processor.result;

import teco.eventMessage.EventMessage;

/**
 * @author u190438
 *
 */
public class DiscardedResult extends ProcessingResult {

	public DiscardedResult(EventMessage event) {
		super(event);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getState() {
		return "discarded";
	}

}
