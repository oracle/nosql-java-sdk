/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Cloud service only.
 * <p>
 * This exception indicates that the provisioned read throughput has been
 * exceeded.
 * <p>
 * Operations resulting in this exception can be retried but it is recommended
 * that callers use a delay before retrying in order to minimize the chance
 * that a retry will also be throttled. Applications should attempt to avoid
 * throttling exceptions by rate limiting themselves to the degree possible.
 * <p>
 * Retries and behavior related to throttling can be managed by configuring
 * the default retry handler using
 * {@link NoSQLHandleConfig#configureDefaultRetryHandler} or by implementing
 * a {@link RetryHandler} and using {@link NoSQLHandleConfig#setRetryHandler}.
 */
public class ReadThrottlingException extends ThrottlingException {

    private static final long serialVersionUID = 1L;

    /**
     * Internal use only
     * @param msg the exception message
     * @hidden
     */
    public ReadThrottlingException(String msg) {
        super(msg);
    }
}
