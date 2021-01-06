/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import oracle.nosql.driver.ops.Request;

/**
 * RetryHandler is called by the request handling system when a
 * {@link RetryableException} is thrown. It controls the number of retries
 * as well as frequency of retries using a delaying algorithm. A default
 * RetryHandler is always configured on a {@link NoSQLHandle} instance and
 * can be controlled or overridden using
 * {@link NoSQLHandleConfig#setRetryHandler} and
 * {@link NoSQLHandleConfig#configureDefaultRetryHandler}
 * <p>
 * It is not recommended that applications rely on a RetryHandler for
 * regulating provisioned throughput. It is best to add rate limiting to the
 * application based on a table's capacity and access patterns to avoid
 * throttling exceptions: see {@link NoSQLHandleConfig#setRateLimitingEnabled}.
 * <p>
 * Instances of this interface must be immutable so they can be shared
 * among threads.
 */
public interface RetryHandler {

    /**
     * Returns the number of retries that this handler instance will allow
     * before the exception is thrown to the application.
     *
     * @return the max number of retries
     */
    int getNumRetries();

    /**
     * This method is called when a {@link RetryableException} is
     * thrown and determines whether to perform a retry or not based
     * on the parameters.
     *
     * @param request the Request that has triggered the exception
     *
     * @param numRetries the number of retries that have occurred for the
     * operation
     *
     * @param re the exception that was thrown
     *
     * @return true if the operation should be retried, false if not, causing
     * the exception to be thrown to the application.
     */
    boolean doRetry(Request request, int numRetries, RetryableException re);

    /**
     * This method is called when a {@link RetryableException} is thrown and it
     * is determined that the request will be retried based on the return value
     * of {@link #doRetry}. It provides a delay between retries. Most
     * implementations will sleep for some period of time. The method should not
     * return until the desired delay period has passed. Implementations should
     * not busy-wait in a tight loop.
     *
     * @param request the Request that has triggered the exception
     *
     * @param numRetries the number of retries that have occurred for the
     * operation
     *
     * @param re the exception that was thrown
     */
    void delay(Request request, int numRetries, RetryableException re);
}
