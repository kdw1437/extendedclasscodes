package com.jurosys.extension.com;

import ch.qos.logback.classic.Logger;
import ch.qos.logback.classic.LoggerContext;
import org.slf4j.LoggerFactory;
import ch.qos.logback.classic.Level;

public class CustomLogger {

    public static Logger enableDebugForLogger(String loggerName) {
        LoggerContext loggerContext = (LoggerContext) LoggerFactory.getILoggerFactory();
        Logger logger = loggerContext.getLogger(loggerName);
        logger.setLevel(Level.DEBUG);
        return logger; // logger instance를 return한다.
    }
}
