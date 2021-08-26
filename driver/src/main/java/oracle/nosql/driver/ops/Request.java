/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.RateLimiter;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;

/**
 * A request is an abstract class used as a base for all requests types.
 * Public state and methods are implemented by extending classes.
 */
public abstract class Request {

    protected int timeoutMs;

    /**
     * The name of the table used for the operation.
     * This is required in most requests. In query requests,
     * it must be set if using internal rate limiting.
     */
    protected String tableName;

    /**
     * Cloud service only.
     *
     * The compartment may be specified as either a name (or path for nested
     * compartments) or as an id (OCID). A name (vs id) can only
     * be used when authenticated using a specific user identity. It is
     * <b>not</b> available if authenticated as an Instance Principal which can
     * be done when calling the service from a compute instance in the Oracle
     * Cloud Infrastructure.  See {@link
     * SignatureProvider#createWithInstancePrincipal}
     */
    protected String compartment;

    /**
     *  @hidden
     */
    private boolean checkRequestSize = true;

    /**
     * @hidden
     */
    private RetryStats retryStats;

    /**
     * @hidden
     */
    private long startTimeMs;

    /**
     * @hidden
     */
    private RateLimiter readRateLimiter;

    /**
     * @hidden
     */
    private RateLimiter writeRateLimiter;
    private int rateLimitDelayedMs;

    protected Request() {}

    /**
     * @hidden
     * @return the timeout in seconds
     */
    public int getTimeoutInternal() {
        return timeoutMs;
    }

    protected void setTimeoutInternal(int timeoutMs) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
        this.timeoutMs = timeoutMs;
    }

    /**
     * @hidden
     * Internal use only.
     *
     * Sets default values in a request based on the specified config
     * object. This will typically be overridden by subclasses.
     *
     * @param config the configuration object to use to get default values
     *
     * @return this
     */
    public Request setDefaults(NoSQLHandleConfig config) {
        if (timeoutMs == 0) {
            timeoutMs = config.getDefaultRequestTimeout();
        }
        return this;
    }

    /**
     * @hidden
     * Return if this request should be retried.
     *
     * @return true if the request should be retried
     */
    public boolean shouldRetry() {
        return true;
    }

    /**
     * @hidden
     * @return true if the request is a query
     */
    public boolean isQueryRequest() {
        return false;
    }

    /**
     * @hidden
     * @return true if the request expects to do reads (incur read units)
     */
    public boolean doesReads() {
        return false;
    }

    /**
     * @hidden
     * @return true if the request expects to do writes (incur write units)
     */
    public boolean doesWrites() {
        return false;
    }

    /**
     * @hidden
     * Internal use only
     * Sets the compartment id or name to use for the operation.
     *
     * @param compartment the compartment id
     */
    public void setCompartmentInternal(String compartment) {
        this.compartment = compartment;
    }

    /**
     * Returns the compartment id or name.
     *
     * Cloud service only.
     *
     * @return compartment id or name if set for the request
     */
    public String getCompartment() {
        return compartment;
    }

    /**
     * @hidden
     * internal use only
     * Sets the table name to use for the operation.
     *
     * @param tableName the table name
     *
     * @return this
     */
    protected void setTableNameInternal(String tableName) {
        this.tableName = tableName;
    }

    /**
     * Returns the table name to use for the operation.
     *
     * @return the table name, or null if not set
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Sets a read rate limiter to use for this request.
     * Cloud service only.
     * <p>
     * This will override any internal rate limiter that may have
     * otherwise been used during request processing, and it will be
     * used regardless of any rate limiter config.
     *
     * @param rl the rate limiter instance to use for read operations
     */
    public void setReadRateLimiter(RateLimiter rl) {
        readRateLimiter = rl;
    }

    /**
     * Sets a write rate limiter to use for this request.
     * Cloud service only.
     * <p>
     * This will override any internal rate limiter that may have
     * otherwise been used during request processing, and it will be
     * used regardless of any rate limiter config.
     *
     * @param rl the rate limiter instance to use for write operations
     */
    public void setWriteRateLimiter(RateLimiter rl) {
        writeRateLimiter = rl;
    }

    /**
     * Returns the read rate limiter instance used during this request.
     * Cloud service only.
     * <p>
     * This will be the value supplied via {@link #setReadRateLimiter}, or if
     * that was not called, it may be an instance of an internal rate
     * limiter that was configured internally during request processing.
     * <p>
     * This is supplied for stats and tracing/debugging only. The returned
     * limiter should be treated as read-only.
     *
     * @return the rate limiter instance used for read operations, or null
     *         if no limiter was used.
     */
    public RateLimiter getReadRateLimiter() {
        return readRateLimiter;
    }

    /**
     * Returns the write rate limiter instance used during this request.
     * Cloud service only.
     * <p>
     * This will be the value supplied via {@link #setWriteRateLimiter}, or if
     * that was not called, it may be an instance of an internal rate
     * limiter that was configured internally during request processing.
     * <p>
     * This is supplied for stats and tracing/debugging only. The returned
     * limiter should be treated as read-only.
     *
     * @return the rate limiter instance used for write operations, or null
     *         if no limiter was used.
     */
    public RateLimiter getWriteRateLimiter() {
        return writeRateLimiter;
    }

    /**
     * @hidden
     * Internal use only
     *
     * This method is called just before a request is sent in order to validate
     * its state.
     * <p>
     * Request instances must implement this in order to validate a request for
     * required parameters, values, etc. On failure they should throw
     * IllegalArgumentException. This method only needs to validate parameters
     * to the point where serialization will work correctly. The proxy will
     * perform additional validatation if required.
     *
     */
    public abstract void validate();

    /**
     * @hidden
     * Internal use only
     *
     * Returns an object that can serialize this request type.
     *
     * @param factory a factory instance used to construct the serializer
     *
     * @return an object used to serialize this request
     */
    public abstract Serializer createSerializer(
        SerializerFactory factory);

    /**
     * @hidden
     * Internal use only
     *
     * Returns an object that can serialize this request type.
     *
     * @param factory a factory instance used to construct the serializer
     *
     * @return an object used to de-serialize this request
     */
    public abstract Serializer createDeserializer(
        SerializerFactory factory);

    /**
     * @hidden
     * @return the value
     */
    public boolean getCheckRequestSize() {
        return checkRequestSize;
    }

    /**
     * @hidden
     * @param value the value
     * @return the value
     */
    public Request setCheckRequestSize(boolean value) {
        checkRequestSize = value;
        return this;
    }

    /**
     * Returns a stats object with information about retries.
     * This may be used during a retry handler or after a
     * request has completed or thrown an exception.
     *
     * @return stats object with retry information, or null if
     *         no retries were performed.
     */
    public RetryStats getRetryStats() {
        return retryStats;
    }

    /**
     * @hidden
     * internal use only
     * This is typically set by internal request processing when the
     * first retry is attempted. It is used/updated thereafter on
     * subsequent retry attempts.
     * @param rs the stats object to use
     */
    public void setRetryStats(RetryStats rs) {
        retryStats = rs;
    }

    /**
     * @hidden
     * internal use only
     * This adds (or increments) a class type to the list of exceptions
     * that were processed during retries of a single request operation.
     * @param re class of exception to add to retry stats
     */
    public void addRetryException(Class<? extends Throwable> re) {
        if (retryStats == null) {
            retryStats = new RetryStats();
        }
        retryStats.addException(re);
    }

    /**
     * @hidden
     * internal use only
     * This adds time to the total time spent processing retries during
     * a single request processing operation.
     * @param millis time to add to retry delay value
     */
    public void addRetryDelayMs(int millis) {
        if (retryStats == null) {
            retryStats = new RetryStats();
        }
        retryStats.addDelayMs(millis);
    }

    /**
     * @hidden
     * internal use only
     * @return time spent in retries, in milliseconds
     */
    public int getRetryDelayMs() {
        if (retryStats == null) {
            return 0;
        }
        return retryStats.getDelayMs();
    }

    /**
     * @hidden
     * internal use only
     * Increments the number of retries during the request operation.
     */
    public void incrementRetries() {
        if (retryStats == null) {
            retryStats = new RetryStats();
        }
        retryStats.incrementRetries();
    }

    /**
     * @hidden
     * internal use only
     * @return number of retries
     */
    public int getNumRetries() {
        if (retryStats == null) {
            return 0;
        }
        return retryStats.getRetries();
    }

    /**
     * @hidden
     * internal use only
     * @param ms start time of request processing
     */
    public void setStartTimeMs(long ms) {
        startTimeMs = ms;
    }

    /**
     * @hidden
     * internal use only
     * @return start time of request processing
     */
    public long getStartTimeMs() {
        return startTimeMs;
    }

    public void setRateLimitDelayedMs(int rateLimitDelayedMs) {
        this.rateLimitDelayedMs = rateLimitDelayedMs;
    }

    public int getRateLimitDelayedMs() {
        return rateLimitDelayedMs;
    }
}
