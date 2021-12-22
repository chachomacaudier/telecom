/**
 * 
 */
package teco.eventMessage.processor.result;

import java.util.logging.Level;

import teco.eventMessage.EventMessage;
import teco.eventMessage.Log;

/**
 * @author u190438
 *
 */
public class WarningResult extends ProcessingResult {

	public WarningResult(EventMessage event) {
		super(event);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getState() {
		return "warning";
	}

	/**
	 * Write a representation of this WARNING execution result to log with WARNING level.
	 * 
	 */
	public void log() {
		Log.getInstance().eventLog("WARNING: " + this.getResultInfo() +
				" SOURCE: " + event.getSource().replaceAll(System.lineSeparator(), ""), Level.WARNING);
	}
}
