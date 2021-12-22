/**
 * 
 */
package teco.eventMessage.processor.result;

import teco.eventMessage.EventMessage;

/**
 * @author u190438
 *
 */
public abstract class ErrorResult extends ProcessingResult {

	public ErrorResult(EventMessage event) {
		super(event);
	}

}
