/**
 * 
 */
package teco.eventMessage.collector.retriever;

import teco.eventMessage.EventMessageException;

/**
 * @author u190438
 *
 */
public class EventRetrieverException extends EventMessageException {

	/**
	 * 
	 */
	private static final long serialVersionUID = 6463716160948723533L;

	/**
	 * 
	 */
	public EventRetrieverException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public EventRetrieverException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public EventRetrieverException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public EventRetrieverException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public EventRetrieverException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
