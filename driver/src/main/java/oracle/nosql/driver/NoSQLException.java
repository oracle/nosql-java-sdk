/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * A base exception for most exceptions thrown by the driver. All of the
 * exceptions defined in this package extend this exception. The driver throws
 * Java exceptions such as {@link IllegalArgumentException} directly.
 */
public class NoSQLException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * internal use only
     * @param msg the message
     * @hidden
     */
    public /*protected*/ NoSQLException(String msg) {
        super(msg + " (" + SDKVersion.VERSION + ")");
    }

    /**
     * internal use only
     * @param msg the message
     * @param cause the cause
     * @hidden
     */
    public /*protected*/ NoSQLException(String msg, Throwable cause) {
        super(msg + " (" + SDKVersion.VERSION + ")", cause);
    }

    /**
     * Returns whether this exception can be retried with a reasonable
     * expectation that it may succeed. Instances of {@link RetryableException}
     * will return true for this method.
     *
     * @return true if this exception can be retried
     */
    public boolean okToRetry() {
        return false;
    }
}
