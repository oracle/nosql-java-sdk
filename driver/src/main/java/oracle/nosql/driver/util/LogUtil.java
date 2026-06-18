/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

import java.util.Arrays;
import java.util.HashSet;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import java.util.logging.Level;
import java.util.logging.Logger;

import io.netty.handler.codec.http.DefaultHttpHeaders;
import io.netty.handler.codec.http.HttpHeaders;

/**
 * Utility methods to facilitate Logging.
 */
public class LogUtil {

    private static final String REDACTED = "<redacted>";
    private static final String LOG_SENSITIVE_HEADERS_PROPERTY =
        "com.oracle.nosql.sdk.nosqldriver.log-sensitive-headers";
    private static final Set<String> SENSITIVE_HEADERS = new HashSet<>(
        Arrays.asList(HttpConstants.AUTHORIZATION.toLowerCase(Locale.ROOT),
                      "proxy-authorization",
                      HttpConstants.COOKIE.toLowerCase(Locale.ROOT),
                      "set-cookie",
                      "opc-obo-token",
                      "security-context"));

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

    public static boolean isSensitiveHeaderLoggingEnabled(Logger logger) {
        if (!isFineEnabled(logger)) {
            return false;
        }
        try {
            return Boolean.getBoolean(LOG_SENSITIVE_HEADERS_PROPERTY);
        } catch (SecurityException se) {
            return false;
        }
    }

    public static void logHeaders(Logger logger,
                                  String label,
                                  HttpHeaders headers) {
        if (isFineEnabled(logger)) {
            logger.log(Level.FINE,
                       label + ": " +
                       formatHeadersForLog(
                           headers,
                           !isSensitiveHeaderLoggingEnabled(logger)));
        }
    }

    public static void logHeaderValue(Logger logger,
                                      String label,
                                      String headerName,
                                      String value) {
        if (isFineEnabled(logger)) {
            logger.log(Level.FINE,
                       label + ": " +
                       formatHeaderValueForLog(
                           headerName,
                           value,
                           !isSensitiveHeaderLoggingEnabled(logger)));
        }
    }

    public static String formatHeadersForLog(HttpHeaders headers) {
        return formatHeadersForLog(headers, true);
    }

    public static String formatHeadersForLog(HttpHeaders headers,
                                             boolean redactSensitive) {
        if (headers == null) {
            return "null";
        }

        final HttpHeaders formatted = new DefaultHttpHeaders(false);
        for (Map.Entry<String, String> entry : headers) {
            formatted.add(entry.getKey(),
                          formatHeaderValueForLog(entry.getKey(),
                                                  entry.getValue(),
                                                  redactSensitive));
        }
        return formatted.toString();
    }

    public static String formatHeaderValueForLog(String headerName,
                                                 String value) {
        return formatHeaderValueForLog(headerName, value, true);
    }

    public static String formatHeaderValueForLog(String headerName,
                                                 String value,
                                                 boolean redactSensitive) {
        if (!redactSensitive || !isSensitiveHeader(headerName)) {
            return value;
        }
        return redactHeaderValue(headerName, value);
    }

    public static String redactHeaderValue(String headerName, String value) {
        if (value == null) {
            return REDACTED;
        }
        final String lowerName = headerName.toLowerCase(Locale.ROOT);
        if (HttpConstants.AUTHORIZATION.toLowerCase(Locale.ROOT)
            .equals(lowerName) ||
            "proxy-authorization".equals(lowerName)) {
            final int space = value.indexOf(' ');
            if (space > 0) {
                return value.substring(0, space + 1) + REDACTED;
            }
        }
        return REDACTED;
    }

    private static boolean isSensitiveHeader(String headerName) {
        return headerName != null &&
               SENSITIVE_HEADERS.contains(
                   headerName.toLowerCase(Locale.ROOT));
    }
}
