package org.littlestar.mysql.common;

import java.io.IOException;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Objects;
import java.util.logging.ConsoleHandler;
import java.util.logging.FileHandler;
import java.util.logging.Formatter;
import java.util.logging.Handler;
import java.util.logging.Level;
import java.util.logging.LogManager;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import java.util.logging.SimpleFormatter;

/**
 * Level mapping:
 * <pre>
 * Java Logger -> Standard Logger Level
 * ALL         -> ALL
 * FINEST      -> TRACE
 * FINER       -> DEBUG
 * FINE        -> DEBUG
 * CONFIG      -> INFO
 * INFO        -> INFO
 * WARNING     -> WARN
 * SEVERE      -> ERROR
 * OFF         -> OFF/No logging.
 * </pre>
 * 
 * @author LiXiang
 *
 */

public class JulConfig {
	final static String DATETIME_FORMAT = "yyyy-MM-dd HH:mm:ss";//.SSSS
	private Level rootLevel = Level.WARNING;
	private final ArrayList<Handler> handlers;

	public JulConfig(Level rootLevel) {
		handlers = new ArrayList<Handler>();
		this.rootLevel = rootLevel;
	}

	public JulConfig withConsoleHandler(Level level) {
		ConsoleHandler consoleHandler = new ConsoleHandler();
		if (Objects.nonNull(level)) {
			consoleHandler.setLevel(level);
		}
		consoleHandler.setFormatter(newSimpleFormatter(DATETIME_FORMAT));
		handlers.add(consoleHandler);
		return this;
	}

	public JulConfig withFileHandler(String pattern, int limit, int count, boolean append, Level level)
			throws SecurityException, IOException {
		FileHandler fileHandler = new FileHandler(pattern, limit, count, append);
		if (Objects.nonNull(level)) {
			fileHandler.setLevel(level);
		}
		fileHandler.setFormatter(newSimpleFormatter(DATETIME_FORMAT));
		handlers.add(fileHandler);
		return this;
	}

	public JulConfig withHandler(Handler handler) {
		if (Objects.nonNull(handler)) {
			handlers.add(handler);
		}
		return this;
	}

	public JulConfig withFileHandler(String logFileName, Level level) throws SecurityException, IOException {
		String pattern = logFileName + ".%g.log";
		int limit = 1024 * 1024 * 2; // 2 MiB
		int count = 4;
		boolean append = true;
		return withFileHandler(pattern, limit, count, append, level);
	}

	public void config() {
		final LogManager logManager = LogManager.getLogManager();
		logManager.reset();
		Logger rootLogger = logManager.getLogger("");
		if (Objects.nonNull(rootLogger)) {
			rootLogger.setLevel(rootLevel);
		}
		for (Handler handler : handlers) {
			if (Objects.nonNull(handler)) {
				rootLogger.addHandler(handler);
			}
		}
	}

	private static Formatter newSimpleFormatter(String dateTimeFormat) {
		final SimpleFormatter formatter = new SimpleFormatter() {
			@Override
			public String format(LogRecord record) {
				StringBuilder msg = new StringBuilder();
				msg.append(formatDateTime(record.getMillis(), dateTimeFormat)).append(" ");
				msg.append("[").append(String.format("%-7s", record.getLevel().getName())).append("] ");
				String className = record.getSourceClassName();
				if (Objects.nonNull(className)) {
					
					String[] pkgs = className.split("\\.");
					StringBuilder nameForShort = new StringBuilder();
					for (int i = 0; i < pkgs.length; i++) {
						if (i < (pkgs.length - 1)) {
							nameForShort.append(pkgs[i].substring(0, 1)).append(".");
						} else {
							nameForShort.append(pkgs[i]);
						}
					}
					msg.append(nameForShort.toString()).append("@").append(record.getSourceMethodName()).append(": ");
				}
				msg.append(formatMessage(record)).append("\n");
				if (Objects.nonNull(record.getThrown())) {
					StringWriter stringWriter = new StringWriter();
					PrintWriter pw = new PrintWriter(stringWriter);
					record.getThrown().printStackTrace(pw);
					pw.close();
					msg.append(stringWriter.toString()).append("\n");
				}
				return msg.toString();
			}
		};
		return formatter;
	}

	private static String formatDateTime(long epochMilli, String pattern) {
		DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern(pattern);
		Instant instant = Instant.ofEpochMilli(epochMilli);
		LocalDateTime localDateTime = LocalDateTime.ofInstant(instant, ZoneId.systemDefault());
		return localDateTime.format(dateTimeFormatter);
	}
}
