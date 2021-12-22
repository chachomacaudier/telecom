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
	 * 
	 * @param logLevel, finest level supported by execution log.
	 * @throws Exception, if an error occurs
	 */
	public static void initializeLog(String logLevel) throws Exception {
		instance = new Log(logLevel);
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

	/**
	 * PRIVATE: Basic constructor
	 */
	private Log(String logLevel) throws Exception {
		DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd.HH.mm.ss");

		String logFileName = "EventCollector-" + LocalDateTime.now().format(formatter) + ".log";
		String currentDirectory = System.getProperty("user.dir");
		FileHandler fh = null;
		try {
			 fh=new FileHandler(Paths.get(currentDirectory, logFileName).toString(), false);
			 fh.setFormatter(new SimpleFormatter());
		} catch (SecurityException | IOException e) {
			throw new Exception("Error opening log file: " + e.getMessage(), e);
		}

		/* Create the execution log and configure it logging level */
		executionLog = Logger.getLogger("EventCollector");
		executionLog.addHandler(fh);
		executionLog.setLevel(Level.parse(logLevel));
		
		logFileName = "event-processing-" + LocalDateTime.now().format(formatter) + ".log";
		try {
			 fh=new FileHandler(Paths.get(currentDirectory, logFileName).toString(), false);
			 fh.setFormatter(new SimpleFormatter() {
		          private static final String format = "[%1$tF %1$tT] [%2$-7s] %3$s %n";

		          @Override
		          public synchronized String format(LogRecord lr) {
		              return String.format(format,
		                      new Date(lr.getMillis()),
		                      lr.getLevel().getName(),
		                      lr.getMessage()
		              );
		          }
		      });
		} catch (SecurityException | IOException e) {
			throw new Exception("Error opening processing log file: " + e.getMessage(), e);
		}
		
		eventsProcessingLog = Logger.getLogger("event-processing");
		eventsProcessingLog.setUseParentHandlers(false);
		eventsProcessingLog.addHandler(fh);
		eventsProcessingLog.setLevel(Level.WARNING);
	}

}
