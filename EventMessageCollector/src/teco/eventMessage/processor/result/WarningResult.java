/**
 * 
 */
package teco.eventMessage.processor.result;

import java.io.IOException;
import java.net.HttpURLConnection;
import java.util.logging.Level;

import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;

import teco.eventMessage.EventMessage;
import teco.eventMessage.Log;
import teco.eventMessage.Utils;

/**
 * @author u190438
 *
 */
public class WarningResult extends ProcessingResult {

	public WarningResult(EventMessage event) {
		super(event);
		// TODO Auto-generated constructor stub
	}

	@Override
	public String getState() {
		return "warning";
	}

	/**
	 * Write a representation of this WARNING execution result to log with WARNING level.
	 * 
	 */
	public void log() {
		Log.getInstance().eventLog("WARNING: " + this.getResultInfo().replaceAll("[\r\n]+", "") +
				" SOURCE: " + event.getSource().replaceAll("[\r\n]+", ""), Level.WARNING);
	}
	
	protected void retrieveResultInfo(HttpURLConnection conn) throws IOException {
		String response = Utils.toString(conn.getInputStream());
		String desc = null;
		JSONObject jsonObj = null;
		JSONArray results = null;
		try {
			jsonObj = new JSONObject(response);
			results = jsonObj.getJSONArray("warnings");
			desc = this.getDescriptionsString(results);
	
		} catch (JSONException ex) {
			// We suppose a worn formatted JSON, probably because the String represents an error message...
			desc = "(Format error) - " + response;
		}
		this.setResultInfo(desc);
	}
	
	private String getDescriptionsString(JSONArray warnings) {
		JSONObject json;
		String descriptions = "";
		
		for (Object warningDesc: warnings) {
			json = (JSONObject) warningDesc;
			descriptions = descriptions + json.getString("description") + "\n";
		}
		return descriptions;
	}
}
