/**
 * 
 */
package teco.eventMessage;

/**
 * @author u190438
 *
 */
public class EventMessageException extends Exception {

	/**
	 * 
	 */
	private static final long serialVersionUID = 3585865792887383519L;

	/**
	 * 
	 */
	public EventMessageException() {
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 */
	public EventMessageException(String message) {
		super(message);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param cause
	 */
	public EventMessageException(Throwable cause) {
		super(cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 */
	public EventMessageException(String message, Throwable cause) {
		super(message, cause);
		// TODO Auto-generated constructor stub
	}

	/**
	 * @param message
	 * @param cause
	 * @param enableSuppression
	 * @param writableStackTrace
	 */
	public EventMessageException(String message, Throwable cause, boolean enableSuppression,
			boolean writableStackTrace) {
		super(message, cause, enableSuppression, writableStackTrace);
		// TODO Auto-generated constructor stub
	}

}
