/**
 * This is the main object, responsible for trigger the message collection, processing and errors handling.
 * 
 */
package teco.eventMessage.collector;

import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.file.Paths;
import java.util.List;
import java.util.Properties;

import teco.eventMessage.EventMessageConfigurationException;
import teco.eventMessage.Log;
import teco.eventMessage.collector.retriever.EventRetriever;
import teco.eventMessage.persistence.EventCollectorPersistenceManager;
import teco.eventMessage.processor.EventProcessor;
import teco.eventMessage.origin.EventMessageOrigin;

/**
 * @author u190438
 *
 */
public class EventCollector {

	private EventCollectorGroup group;

	/**
	 * Create a new instance configured from the passed properties.
	 * - Initialize the persistence layer
	 * - Retrieve the EventCollectorGroup needed for this run.
	 * 
	 */
	public EventCollector(Properties properties) throws Exception {
		String ecg_name;
		
		ecg_name = properties.getProperty("collector_group");
		if (ecg_name == null)
			throw new EventMessageConfigurationException("Required configuration property <collector_group> missing!");
		/* Initialize th persistence manager */
		EventCollectorPersistenceManager.initialize(properties);
		group = EventCollectorPersistenceManager.getInstance().retrieveEventCollectorGroup(ecg_name);
	}

	/**
	 * Retrieve events of all origins by origin group order.
	 * The retrieved events are stored in pending status. 
	 * 
	 * @throws Exception, if a connection error occurs during retrieving (origin connection or internal persistence connection)
	 *  
	 */
	public void retrieveNewEvents() throws Exception {
		List<EventMessageOrigin> origins = group.getSortedOriginList();
		int eventCount = 0;
		
		Log.getInstance().info("Starting dequeue process..");
		for (EventMessageOrigin origin : origins) {
			EventRetriever retriever = new EventRetriever(origin);
			eventCount += retriever.retrieveEvents();
		}
		Log.getInstance().info("Finish dequeue process (TOTAL EVENTS count: " + Integer.toString(eventCount) + ")");
	}

	/**
	 * Process all processable events.
	 * The internal state needed for resuming in future execution is stored.
	 * 
	 * @throws Exception, if an error occurs.
	 */
	public void processEvents() throws Exception {
		EventProcessor processor = new EventProcessor(group);

		Log.getInstance().info("Starting processing of events..");
		int eventCount = processor.executeProcessableEvents();
		Log.getInstance().info("Finish processing (TOTAL EVENTS count: " + Integer.toString(eventCount) + ")");
	}

	/**
	 * Release all resources.
	 * 
	 */
	public void release() {
		EventCollectorPersistenceManager.releaseInstance();
	}

	/**
	 * Start collector process.
	 * - retrieve available events in order of all group origins.
	 * - If group reference the last processed event message:
	 * 	 	process pending events after this, previously stored (in current run or previous one)
	 * - if group reference a retryable event message:
	 * 		process pending events from this one and all pending events
	 * - Set last event processed in group for future executions or retryable event if fails.
	 * 
	 * @param args
	 */
	public static void main(String[] args) {

		if(args.length != 1)
		{
			System.out.println("No se especifico un archivo de configuracion.");
			System.exit(1);
		}

		/* Load configuration properties */
		EventCollector collector = null;
		String configFileName = args[0];
		String currentDirectory = System.getProperty("user.dir");
		Properties configProp = new Properties();

		try  {
			InputStream in = new FileInputStream(Paths.get(currentDirectory, configFileName).toString());
			configProp.load(in);
			String logLevel = configProp.getProperty("log_level");
			Log.initializeLog(logLevel);
		} catch (Exception e) {
			System.out.println("Error initializing log: " + e.getMessage());
			System.exit(1);
		}
		
		/* Create a new EventCollector */
		try  {
			collector = new EventCollector(configProp);
		} catch (Exception e) {
			Log.getInstance().error(e.getMessage(), e);
			System.exit(1);
		}

		int collectStatus = 0;
		int processStatus = 0;

		/* Retrieve all available events */
		try {
			collector.retrieveNewEvents();
		} catch (Exception ex) {
			Log.getInstance().error(ex.getMessage(), ex);
			collectStatus = 1;
		} finally {
			Log.getInstance().info("Collection finished "+ (collectStatus == 0?"Ok":"with ERRORS"));
		}

		/* Process all 'pending' events */
		try {
			collector.processEvents();
		} catch (Exception ex) {
			Log.getInstance().error(ex.getMessage(), ex);
			processStatus = 1;
		} finally {
			Log.getInstance().info("Processing finished "+ (processStatus == 0?"Ok":"with ERRORS"));
		}

		/* Release resources */
		if (collector != null) {
			Log.getInstance().info("Releasing collector...");
			collector.release();
		}
		
		System.exit(Integer.max(collectStatus, processStatus));
	}
}
