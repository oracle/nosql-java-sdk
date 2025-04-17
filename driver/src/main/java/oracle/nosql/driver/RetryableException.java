/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * A base exception for all exceptions that may be retried with a reasonable
 * expectation that they may succeed on retry.
 */
public class RetryableException extends NoSQLException {

    private static final long serialVersionUID = 1L;

    /**
     * @hidden
     */
    protected RetryableException(String msg) {
        super(msg);
    }

    /**
     * @hidden
     */
    protected RetryableException(String msg, Throwable cause) {
        super(msg, cause);
    }

    @Override
    public boolean okToRetry() {
        return true;
    }
}
