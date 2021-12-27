/**
 * 
 */
package teco.eventMessage.processor.result;

import java.io.IOException;
import java.net.HttpURLConnection;

import org.json.JSONException;
import org.json.JSONObject;

import teco.eventMessage.EventMessage;
import teco.eventMessage.Utils;

/**
 * @author u190438
 *
 */
public abstract class ErrorResult extends ProcessingResult {

	public ErrorResult(EventMessage event) {
		super(event);
	}

	protected void retrieveResultInfo(HttpURLConnection conn) throws IOException {
		String response = Utils.toString(conn.getErrorStream());
		String desc = null;
		JSONObject jsonObj = null;
		try {
			jsonObj = new JSONObject(response);
			desc = jsonObj.getString("errorMessage");
	
		} catch (JSONException ex) {
			// We suppose a worn formatted JSON, probably because the String represents an error message...
			desc = "(Format error) - " + response;
		}
		this.setResultInfo(desc);
	}
}
