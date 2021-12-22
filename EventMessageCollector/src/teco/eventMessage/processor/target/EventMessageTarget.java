/**
 * Represents the target of processing a message for an origin.
 * Could be re-used by severals origins.
 * 
 */
package teco.eventMessage.processor.target;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.json.JSONObject;

import teco.eventMessage.EventMessage;
import teco.eventMessage.Utils;
import teco.eventMessage.processor.result.ProcessingResult;

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
	 * Send to destination endPoint the event configured operation with
	 * event as body.
	 * 
	 * Catch ALL possible errors, generate the most representative result and
	 * return it.
	 * 
	 * Each obtained result is stored in the events table and correctly updated in
	 * their group.
	 * 
	 * @param event, an EventMessage instance ready to be executed.
	 * @return a ProcessingResult, the execution result.
	 */
	public ProcessingResult executeEventMessage(EventMessage event) {
		// TODO
		return null;
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


	/************************************************************
	 * 
	 * General private methods
	 * 
	 ************************************************************/

}
