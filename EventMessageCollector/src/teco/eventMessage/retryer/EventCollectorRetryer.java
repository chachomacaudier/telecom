/**
 * Retrying messages finished with business errors functionality.
 */
package teco.eventMessage.retryer;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Map;
import java.util.Properties;

import teco.eventMessage.EventMessage;
import teco.eventMessage.EventMessageConfigurationException;
import teco.eventMessage.Log;
import teco.eventMessage.collector.EventCollectorGroup;
import teco.eventMessage.persistence.EventCollectorPersistenceManager;
import teco.eventMessage.persistence.EventMessagePersistenceException;
import teco.eventMessage.processor.EventProcessor;

/**
 * @author u190438
 *
 * Analyze messages in business error inside a period window.
 * Invoke Obsoleter if have no sense re-processing found messages or try to process each again.
 * 
 */
public class EventCollectorRetryer {

	private EventCollectorGroup group;
	private long initialId;

	/**
	 * Create a new instance ready to re-process on error messages.
	 * 
	 * @param properties, configuration file properties to configure the system
	 * @throws Exception, if an error occurs
	 * 
	 */
	public EventCollectorRetryer(Properties properties) throws Exception {
		String ecg_name;
		
		ecg_name = properties.getProperty("collector_group");
		if (ecg_name == null)
			throw new EventMessageConfigurationException("Required configuration property <collector_group> missing!");
		/* Initialize the persistence manager */
		EventCollectorPersistenceManager.initialize(properties);
		group = EventCollectorPersistenceManager.getInstance().retrieveEventCollectorGroup(ecg_name);
	}

	/**
	 * Analyze all messages with status='error' from group defined window period.
	 * 
	 * For each different element from found messages, determine best action to execute:
	 * - if their even messages have no sense to be re-processed, then mark them as 'obsolet'.
	 * if are not obsolet there is a possibility of successful processing, so try to do that.
	 * 
	 * @throws Exception if an error occurs.
	 * 
	 */
	public void retryOnErrorMessages() throws Exception {
		Log.getInstance().info("Starting retrying process..");
		try {
			this.calculateInitialOnErrorID();
		} catch(OnErrorEventMessageNotFound ex) {
			Log.getInstance().info("No events on error in window period.");
			return;
		}

		/* Retrieving first IDs of potential elements to re-process */
		Log.getInstance().info("Getting elelements to analyze...");
		Map<String, Long> firstIDs = group.getOnErrorMessageIDsByElement(initialId);
		Log.getInstance().info(Integer.toString(firstIDs.size()) + " different elements found.");

		/* Determine for each element if their messages are obsolet and mark them if appropriate */
		for (Map.Entry<String, Long> pair : firstIDs.entrySet()) {
			this.markAsObsoletIfAppropriate(pair.getKey(), pair.getValue().longValue());
		};

		/* If still remind messages on error inside window period, re-process them in order */
		this.reProcessOnErrorMessages();
	}

	/**
	 * Retrieve all window period remaining messages on error and execute them.
	 * 
	 * @throws Exception, if an error occurs.
	 */
	private void reProcessOnErrorMessages() throws Exception {
		EventProcessor processor = new EventProcessor(group);

		Log.getInstance().info("Starting re-processing of remaining on error events..");
		List<EventMessage> events = group.getReProcessableEvents(initialId);
		int eventCount = processor.executeEvents(events, false);
		Log.getInstance().info("Finish re-processing (TOTAL EVENTS count: " + Integer.toString(eventCount) + ")");
	}

	/**
	 * Determine for the element (with identification) if their messages (from the startId) are obsolet 
	 * and mark them if appropriate.
	 * 
	 * @param identification, String with element identification
	 * @param statId, long with id from which start the analysis and marking.
	 * @throws EventMessagePersistenceException, if an error occurs.
	 * 
	 */
	private void markAsObsoletIfAppropriate(String identification, long statId) throws EventMessagePersistenceException {
		Long okID = null;
		Log.getInstance().info("Analyzing element: " + identification);
		okID = group.getSuccessfulyProcessedMessageIDAfter(statId, identification);
		if (okID == null) {
			Log.getInstance().info("No successfuly processed message foud for element: " + identification);
		} else {
			int obsoletCount = group.setObsoletOnErrorMessagesOfElement(identification, statId, okID.longValue());
			Log.getInstance().info(Integer.toString(obsoletCount) + " messages of element: " + identification + " marked as OBSOLET");
		}
	}

	/**
	 * Release all resources.
	 * 
	 */
	public void release() {
		EventCollectorPersistenceManager.releaseInstance();
	}

	/******************************************************************************
	 * 
	 * General Private methods
	 * 
	 ******************************************************************************/

	/**
	 * Analyze previous messages inside group failedEventRetryable window and set the initial.
	 * From there will retry processing if don't have to set as obsolet.
	 */
	private void calculateInitialOnErrorID() throws OnErrorEventMessageNotFound {
		initialId = group.getInitialOnErrorEventMessageID();
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if(args.length != 1)
		{
			System.out.println("No se especifico un archivo de configuracion.");
			System.exit(1);
		}

		/* Load configuration properties */
		EventCollectorRetryer retryer = null;
		String configFileName = args[0];
		Properties configProp = new Properties();

		try  {
			InputStream in = new FileInputStream(Paths.get(configFileName).toString());
			configProp.load(in);
			String logLevel = configProp.getProperty("log_level");
			Log.initializeLog(logLevel, "EventRetryer", "event-retryer");
		} catch (Exception e) {
			System.out.println("Error initializing log: " + e.getMessage());
			System.exit(1);
		}
		
		/* Create a new EventCollectorRetryer */
		try  {
			retryer = new EventCollectorRetryer(configProp);
		} catch (Exception e) {
			Log.getInstance().error(e.getMessage(), e);
			System.exit(1);
		}

		int status = 0;

		/* Retry all on 'error' events inside group window period */
		try {
			retryer.retryOnErrorMessages();
		} catch (Exception ex) {
			Log.getInstance().error(ex.getMessage(), ex);
			status = 1;
		} finally {
			Log.getInstance().info("Retrying finished "+ (status == 0?"Ok":"with ERRORS"));
		}

		/* Release resources */
		if (retryer != null) {
			Log.getInstance().info("Releasing retryer...");
			retryer.release();
		}
		
		System.exit(status);
	}

}
