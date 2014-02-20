package com.github.davidmoten.rx.jdbc;

import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;

/**
 * Custom logging (did not use one of the standard frameworks to avoid
 * dependencies but might consider switch to slf4j).
 * 
 */
final class Logger {

	// TODO turn off by default
	private static boolean debugEnabled = false;

	/**
	 * Set the parameter to true to enable logging of database and observable
	 * events.
	 * 
	 * @param enable
	 */
	public static void debugLogging(boolean enable) {
		debugEnabled = enable;
	}

	/**
	 * Name of the logger to be included in the log line.
	 */
	private final String name;

	/**
	 * Constructor.
	 * 
	 * @param name
	 */
	private Logger(String name) {
		this.name = name;
	}

	/**
	 * Factory method to construct instances. Returns a new instance of Logger
	 * using the class name in each log line.
	 * 
	 * @param cls
	 * @return
	 */
	public static Logger getLogger(Class<?> cls) {
		return new Logger(cls.getSimpleName());
	}

	/**
	 * Writes the toString value of the parameter to the log prefixed by
	 * timestamp and thread name.
	 * 
	 * @param msg
	 */
	void debug(Object msg) {
		SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
		sdf.setTimeZone(TimeZone.getTimeZone("UTC"));
		if (debugEnabled)
			System.out.println(sdf.format(new Date()) + " " + name + " - "
					+ Thread.currentThread().getName() + " - " + msg);
	}
}
