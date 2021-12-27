/**
 * Represents the target of processing a message for an origin.
 * Could be re-used by severals origins.
 * 
 */
package teco.eventMessage.processor.target;

import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import teco.eventMessage.EventMessage;
import teco.eventMessage.processor.result.ProcessingResult;
import teco.eventMessage.processor.result.RetryableErrorResult;

/**
 * @author u190438
 *
 */
public class EventMessageTarget {

	private long id;
	private String name;
	private int timeout;
	private int max_retries;
	private String endPointURLString;
	private URL endPointURL;
	private EventMessageTargetToken token;

	public static String db_type = "EventMessageTarget";

	/**************************************************
	 *  Cache of reusable tokens indexed by tokenURL
	 * */
	private static Map<String, EventMessageTargetToken> tokens = new HashMap<String, EventMessageTargetToken>();

	public static EventMessageTargetToken getToken(String url) {
		return tokens.get(url);
	}
	
	public static void addToken(EventMessageTargetToken token) {
		tokens.put(token.getURL(), token);
	}

	/**************************************************
	 * Basic constructor.
	 * 
	 * Create an incomplete instance.
	 * 
	 **************************************************/
	public EventMessageTarget(long _id, String _name) {
		id = _id;
		name = _name;
	}

	/**
	 * Send to destination endPoint (REST Service) the configured event operation with
	 * event JSON source as body.
	 * 
	 * Catch ALL possible errors, generate the most representative result and
	 * return it.
	 * 
	 * IMPORTAN: Each obtained result should be applied to originator event and correctly updated in
	 * their group (responsibility of invoker).
	 * 
	 * @param event, an EventMessage instance ready to be executed.
	 * @return a ProcessingResult, the execution result.
	 */
	public ProcessingResult executeEventMessage(EventMessage event) {
		HttpURLConnection conn = null;
		ProcessingResult result = null;
		if (token.isOnError())
			return RetryableErrorResult.fromTokenError(event, token.getErrorDescription());

		try {
			conn = (HttpURLConnection) this.endPointURL.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod(event.getOperation().getRESTMethod());
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Authorization", "Bearer " + token.getToken());
			conn.setConnectTimeout(timeout);
			conn.setReadTimeout(timeout);
	
			OutputStream os = conn.getOutputStream();
			os.write(event.getSource().getBytes(StandardCharsets.UTF_8));
			os.flush();
	
			int responseCode = conn.getResponseCode();
			
			result = ProcessingResult.fromResponse(responseCode, conn, event);
		} catch (Exception ex) {
			result = ProcessingResult.resultFromException("Unexpected Error (" + (ex.getClass().getName()) + ") - " + ex.getMessage(), event);
		} finally {
			conn.disconnect();
		}

		return result;
	}

	/***************************************************
	 * Attributes Accessors
	 * 
	 ***************************************************/

	public int getTimeout() {
		return timeout;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public int getMax_retries() {
		return max_retries;
	}

	public void setMax_retries(int max_retries) {
		this.max_retries = max_retries;
	}

	public void setEndPointURL(String endPointURL) throws EventMessageTragetException {
		this.endPointURLString = endPointURL;
		try {
			this.endPointURL = new URL(this.endPointURLString);
		} catch (MalformedURLException ex) {
			throw new EventMessageTragetException("EventMessageTarget creation ERROR:" + ex.getMessage(), ex);
		}
	}

	public long getId() {
		return id;
	}

	public String getName() {
		return name;
	}

	public void setEndPointURLString(String endPointURLString) {
		this.endPointURLString = endPointURLString;
	}

	public void setToken(EventMessageTargetToken token) {
		this.token = token;
	}
}
