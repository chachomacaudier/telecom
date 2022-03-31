/**
 * Token functionality for use from EventMessageTarget.
 * 
 */
package teco.eventMessage.processor.target;

import java.io.IOException;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Set;

import org.json.JSONObject;

import teco.eventMessage.Utils;

/**
 * @author u190438
 *
 */
public class EventMessageTargetToken {
	private int timeout;
	private String tokenURLString;
	private String user;
	private String password;
	private URL tokenURL;
	private String token;
	private boolean onError = false;
	private String errorDescription = null;

	private static Set<Integer> validTokenResponses = new HashSet<Integer>();
	static { 
		validTokenResponses.addAll(Arrays.asList(
            new Integer[] { HttpURLConnection.HTTP_CREATED, HttpURLConnection.HTTP_OK }));
	}

	/**
	 * Creates a new configured instance.
	 * Needs to be prepared before it will be used for target processing.
	 * 
	 */
	public EventMessageTargetToken(String url, String user, String password, int timeout)  throws EventMessageTragetException {
		this.tokenURLString = url;
		this.user = user;
		this.password = password;
		this.timeout = timeout;
		try {
			this.tokenURL = new URL(tokenURLString);
		} catch (MalformedURLException ex) {
			throw new EventMessageTragetException("EventMessageTarget token creation ERROR:" + ex.getMessage(), ex);
		}
	}

	/**
	 * Request a new token if was not previously obtained.
	 * Set onError and errorDescription if something was wrong.
	 * 
	 */
	public void prepareForProcessing() {
		if (token != null) return;

		try {
			this.requestNewToken();
		} catch (EventMessageTragetTokenException ex) {
			onError = true;
			errorDescription = ex.getMessage();
		}
	}

	/*********************************************************
	 * Private methods
	 *********************************************************/

	private void requestNewToken() throws EventMessageTragetTokenException {
		HttpURLConnection conn = null;
		
		try {
			conn = (HttpURLConnection) this.tokenURL.openConnection();
			conn.setDoOutput(true);
			conn.setRequestMethod("POST");
			conn.setRequestProperty("Accept", "application/json");
			conn.setRequestProperty("Content-Type", "application/json");
			conn.setConnectTimeout(timeout);
			conn.setReadTimeout(timeout);
	
			String input = "{ \"user\":\"" + this.user + "\", " + 
					"\"password\":\"" + this.password + "\" }";
	
			OutputStream os = conn.getOutputStream();
			os.write(input.getBytes());
			os.flush();
	
			if (!validTokenResponses.contains(Integer.valueOf(conn.getResponseCode()))) {
				throw new EventMessageTragetTokenException("Error retrieving token: HTTP error code : "
					+ Integer.toString(conn.getResponseCode()) + " description: " + conn.getResponseMessage());
			}
	
			JSONObject tokenObj = new JSONObject(Utils.toString(conn.getInputStream()));
			
			this.token = tokenObj.getString("token");

		} catch (IOException ex) {
			throw new EventMessageTragetTokenException("Error retrieving token - " + ex.getMessage(), ex);
		} finally {
			conn.disconnect();
		}
		
	}

	/*********************************************************
	 * Getters and Setters
	 * 
	 *********************************************************/

	/**
	 * Used for identify specific Token
	 * 
	 * @return tokenURLString
	 */
	public String getURL() {
		return tokenURLString;
	}

	public String getToken() {
		return token;
	}

	public boolean isOnError() {
		return onError;
	}

	public String getErrorDescription() {
		return errorDescription;
	}

	public void setTimeout(int timeout) {
		this.timeout = timeout;
	}

	public void setTokenURLString(String tokenURLString) {
		this.tokenURLString = tokenURLString;
	}

	public void setUser(String user) {
		this.user = user;
	}

	public void setPassword(String password) {
		this.password = password;
	}
}
