/**
 * Log access for entire application
 * 
 */
package teco.eventMessage;

import java.io.IOException;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Date;
import java.util.logging.FileHandler;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * @author u190438
 *
 */
public class Log {
	private static Logger executionLog, eventsProcessingLog;

	/**
	 * Singleton implementation
	 * 
	 * */
	private static Log instance;

	public static Log getInstance() {
		return instance;
	}
	
	/**
	 * Log initialization, should be called before getting single instance.
	 * Set default prefixes for retro-compatibility.
	 * 
	 * @param logLevel, finest level supported by execution log.
	 * @throws Exception, if an error occurs
	 */
	public static void initializeLog(String logLevel) throws Exception {
		initializeLog(logLevel, "EventCollector", "event-processing");
	}

	/**
	 * Log initialization, should be called before getting single instance.
	 * 
	 * @param logLevel, finest level supported by execution log.
	 * @param execution_prefix, prefix name used for define execution log name.
	 * @param processing_prefix, prefix name used for define processing log name.
	 * @throws Exception, if an error occurs
	 */
	public static void initializeLog(String logLevel, String execution_prefix, String processing_prefix) throws Exception {
		instance = new Log(logLevel, execution_prefix, processing_prefix);
	}

	/* ********************************
	 * 
	 * Basic execution logging methods
	 * 
	 * ********************************/

	public void info(String message) {
		executionLog.info(message);
	}

	public void warning(String message) {
		executionLog.warning(message);
	}

	public void warning(String message, Throwable thrown) {
		executionLog.log(Level.WARNING, message, thrown);
	}

	public void error(String message, Throwable thrown) {
		executionLog.log(Level.SEVERE, message, thrown);
	}

	/* *********************************
	 * 
	 * Event processing logging methods
	 * 
	 * *********************************/
	
	public void eventLog(String message, Level level) {
		eventsProcessingLog.log(level, message);
	}

	public void eventLog(String message, Level level, Throwable thrown) {
		eventsProcessingLog.log(level, message, thrown);
	}

	private Log(String logLevel, String execution_prefix, String processing_prefix) throws Exception {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH.mm.ss");

		String logFileName = execution_prefix + "-" + LocalDateTime.now().format(formatter) + ".log";
		String currentDirectory = Paths.get(".").toAbsolutePath().normalize().toString();
		FileHandler fh = null;
		try {
			 fh=new FileHandler(Paths.get(currentDirectory, logFileName).toString(), false);
			 fh.setFormatter(new SimpleFormatter());
		} catch (SecurityException | IOException e) {
			throw new Exception("Error opening log file: " + e.getMessage(), e);
		}

		/* Create the execution log and configure it logging level */
		executionLog = Logger.getLogger(execution_prefix);
		executionLog.addHandler(fh);
		executionLog.setLevel(Level.parse(logLevel));
		
		logFileName = processing_prefix + "-" + LocalDateTime.now().format(formatter) + ".log";
		try {
			 fh=new FileHandler(Paths.get(currentDirectory, logFileName).toString(), false);
			 fh.setFormatter(new SimpleFormatter() {
				 /**
				  * Custom Format for processing log:
				  *  
				  *  date time|level|<message>
				  *  <message> -> description|origin|elem_type|elem_id|source
				  *  
				  **/
		          private static final String format = "%1$tF %1$tT|%2$s|%3$s %n";

		          @Override
		          public synchronized String format(LogRecord lr) {
		              return String.format(format,
		                      new Date(lr.getMillis()),
		                      (lr.getLevel() == Level.SEVERE ? "ERROR" : lr.getLevel().getName()),
		                      lr.getMessage()
		              );
		          }
		      });
		} catch (SecurityException | IOException e) {
			throw new Exception("Error opening processing log file: " + e.getMessage(), e);
		}
		
		eventsProcessingLog = Logger.getLogger(processing_prefix);
		eventsProcessingLog.setUseParentHandlers(false);
		eventsProcessingLog.addHandler(fh);
		eventsProcessingLog.setLevel(Level.WARNING);
	}

	/**
	 * PRIVATE: Basic constructor
	 */
	private Log(String logLevel) throws Exception {
		this(logLevel, "EventCollector", "event-processing");
	}

}
