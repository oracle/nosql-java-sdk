/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
package oracle.nosql.driver;

/**
 * Thrown when a request cannot be processed because the configured timeout
 * interval is exceeded. If a retry handler is configured it is possible that
 * the request has been retried a number of times before the timeout occurs.
 */
public class RequestTimeoutException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     */
    private volatile int timeoutMs;

    /**
     * @hidden
     * Internal use only.
     *
     * @param msg the message string for the timeout
     */
    public RequestTimeoutException(String msg) {
        this(0, msg, null);
    }

    /**
     * @hidden
     * Internal use only.
     *
     * @param timeoutMs the timeout that was in effect, in milliseconds
     * @param msg the message string for the timeout
     */
    public RequestTimeoutException(int timeoutMs,
                                   String msg) {
        this(timeoutMs, msg, null);
    }

    /**
     * @hidden
     * Internal use only.
     *
     * @param timeoutMs the timeout that was in effect, in milliseconds
     * @param msg the message string for the timeout
     * @param cause the cause of the exception
     */
    public RequestTimeoutException(int timeoutMs,
                                   String msg,
                                   Throwable cause) {
        super(msg, cause);
        this.timeoutMs = timeoutMs;
    }

    @Override
    public String getMessage() {
        StringBuilder sb = new StringBuilder();
        sb.append(super.getMessage());
        if (timeoutMs != 0) {
            sb.append(" Timeout: ");
            sb.append(timeoutMs);
            sb.append("ms");
        }

        Throwable cause = getCause();
        if (cause != null) {
            sb.append("\nCaused by: ");
            sb.append(cause.getClass().getName());
            sb.append(": ");
            sb.append(cause.getMessage());
        }
        return sb.toString();
    }

    /**
     * Returns the timeout that was in effect for the operation.
     *
     * @return the timeout that was in use for the operation that timed out,
     * in milliseconds, if the timeout is not known it will be 0.
     */
    public int getTimeoutMs() {
        return timeoutMs;
    }
}
