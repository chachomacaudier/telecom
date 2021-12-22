package teco.eventMessage.processor.result;

import java.util.logging.Level;

import teco.eventMessage.EventMessage;
import teco.eventMessage.Log;

public class BusinessErrorResult extends ErrorResult {

	public BusinessErrorResult(EventMessage event) {
		super(event);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getState() {
		return "error";
	}

	/**
	 * Write a representation of this ERROR execution result to log with SEVERE level.
	 * 
	 */
	public void log() {
		Log.getInstance().eventLog("ERROR (business): " + this.getResultInfo() +
				" SOURCE: " + event.getSource().replaceAll(System.lineSeparator(), ""), Level.SEVERE);
	}

}
