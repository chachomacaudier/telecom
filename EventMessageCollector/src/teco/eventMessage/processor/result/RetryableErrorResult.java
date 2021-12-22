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
}
