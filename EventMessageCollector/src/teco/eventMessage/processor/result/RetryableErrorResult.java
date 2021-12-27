package teco.eventMessage.processor.result;

import teco.eventMessage.EventMessage;

public class RetryableErrorResult extends ErrorResult {

	public RetryableErrorResult(EventMessage event) {
		super(event);
		// TODO Auto-generated constructor stub
	}

	public boolean shouldAbortProcessing() {
		return false;
	}

	@Override
	public String getState() {
		return "retryable";
	}

	/**
	 * Create a new instance for passed event and resultInfo as passed erroDescription.
	 * 
	 * @param event
	 * @param errorDescription
	 * @return a new RetryableErrorResult
	 */
	public static RetryableErrorResult fromTokenError(EventMessage event, String errorDescription) {
		RetryableErrorResult result = new RetryableErrorResult(event);
		result.setResultInfo("Retryable error: " + errorDescription);
		return result;
	}
}
