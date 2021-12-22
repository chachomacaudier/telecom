package teco.eventMessage.processor;

import teco.eventMessage.EventMessageException;

public class EventMessageProcessingException extends EventMessageException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 160485334279101889L;

	public EventMessageProcessingException() {
		// TODO Auto-generated constructor stub
	}

	public EventMessageProcessingException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	public EventMessageProcessingException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	public EventMessageProcessingException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	public EventMessageProcessingException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
