/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import oracle.nosql.driver.ops.Request;

/**
 * Default retry handler.
 * This may be extended by clients for specific use cases.
 *
 * The default retry handler decides when and for how long retries will
 * be attempted. See {@link RetryHandler} for more information on
 * retry handlers.
 */
public class DefaultRetryHandler implements RetryHandler {

    private final int maxRetries;
    private final int fixedDelayMs;

    DefaultRetryHandler(int retries, int delayMS) {
        if (retries < 0) {
            throw new IllegalArgumentException(
                "Retry handler: number of retries must " +
                "be a non-negative value");
        }
        if (delayMS < 0) {
            throw new IllegalArgumentException(
                "Retry handler: delay milliseconds must " +
                "be a non-negative value");
        }
        this.fixedDelayMs = delayMS;
        this.maxRetries = retries;
    }

    @Override
    public int getNumRetries() {
        return maxRetries;
    }

    /**
     * Decide whether to retry or not.
     * Default behavior is to *not* retry OperationThrottlingException
     * because the retry time is likely much longer than normal because
     * they are DDL operations. Read and Write throttling exceptions are
     * always retryable. Otherwise check the request itself to see if
     * it should not be retried.
     */
    @Override
    public boolean doRetry(Request request,
                           int numRetries,
                           RetryableException re) {
        if (re instanceof OperationThrottlingException) {
            return false;
        }
        if (re instanceof ReadThrottlingException ||
            re instanceof WriteThrottlingException ||
            request.shouldRetry()) {
            return (numRetries < maxRetries);
        }
        return false;
    }

    /**
     * Delay (sleep) during retry cycle.
     * If delayMS is non-zero, use it. Otherwise, use an incremental backoff
     * algorithm to compute the time of delay.
     */
    @Override
    public void delay(Request request,
                      int numRetries,
                      RetryableException re) {

        int delayMs = computeBackoffDelay(request, fixedDelayMs);
        if (delayMs <= 0) {
            return;
        }
        try {
            Thread.sleep(delayMs);
        } catch (InterruptedException ie) {}
        request.addRetryDelayMs(delayMs);
    }

    /**
     * Compute an incremental backoff delay in milliseconds.
     * This method also checks the request's timeout and ensures the
     * delay will not exceed the specified timeout.
     *
     * @param request The request object being executed
     * @param fixedDelayMs A specific delay to use and check for timeout.
     *        Pass zero to use the default backoff logic.
     *
     * @return The number of milliseconds to delay. If zero,
     *         do not delay at all.
     */
    public static int computeBackoffDelay(Request request, int fixedDelayMs) {
        int delayMs = fixedDelayMs;
        if (delayMs == 0) {
            /* add 200ms plus a small random amount */
            int mSecToAdd = 200 + (int)(Math.random() * 50);

            delayMs = request.getRetryDelayMs();
            delayMs += mSecToAdd;
        }

        /*
         * if the delay would put us over the timeout, reduce it to just before
         * the timeout would occur.
         */
        long nanosUsed = System.nanoTime() - request.getStartNanos();
        int msUsed = Math.toIntExact(nanosUsed / 1_000_000);
        int timeoutMs = request.getTimeoutInternal();
        int msLeft = (timeoutMs - msUsed) - 1;
        if (msLeft < delayMs) {
            delayMs = msLeft;
            if (delayMs < 1) {
                return 0;
            }
        }

        return delayMs;
    }
}
