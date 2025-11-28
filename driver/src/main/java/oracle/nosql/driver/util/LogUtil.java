/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 * Utility methods to facilitate Logging.
 */
public class LogUtil {

    public static boolean isFineEnabled(Logger logger) {
        return logger != null && logger.isLoggable(Level.FINE);
    }

    public static void logSevere(Logger logger, String msg) {
        if (logger != null) {
            logger.log(Level.SEVERE, msg);
        }
    }

    public static void logSevere(Logger logger, String msg, Throwable thrown) {
        if (logger != null) {
            logger.log(Level.SEVERE, msg, thrown);
        }
    }

    public static void logWarning(Logger logger, String msg) {
        if (logger != null) {
            logger.log(Level.WARNING, msg);
        }
    }

    public static void logWarning(Logger logger, String msg, Throwable thrown) {
        if (logger != null) {
            logger.log(Level.WARNING, msg, thrown);
        }
    }

    public static void logInfo(Logger logger, String msg) {
        if (logger != null) {
            logger.log(Level.INFO, msg);
        }
    }

    public static void logFine(Logger logger, String msg) {
        if (logger != null) {
            logger.log(Level.FINE, msg);
        }
    }

    /**
     * Trace == FINE
     */
    public static void logTrace(Logger logger, String msg) {
        if (logger != null) {
            logger.log(Level.FINE, msg);
        }
    }

    public static boolean isLoggable(Logger logger, Level level) {
        return (logger != null && logger.isLoggable(level));
    }

    /**
     * Returns the stack trace.
     *
     * @param t the exception
     */
    public static String getStackTrace(Throwable t) {
        if (t == null) {
            return null;
        }
        final StringWriter sw = new StringWriter();
        final PrintWriter pw = new PrintWriter(sw);
        t.printStackTrace(pw);
        return sw.toString();
    }
}
