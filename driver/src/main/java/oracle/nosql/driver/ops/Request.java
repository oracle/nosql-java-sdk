/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
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
     * On-premises use only.
     *
     * Set the namespace to use for the operation. Note: if a namespace is
     * also specified in the table name, that namespace will override this one.
     */
    protected String namespace;

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
    private long startNanos;

    /**
     * @hidden
     */
    private RateLimiter readRateLimiter;

    /**
     * @hidden
     */
    private RateLimiter writeRateLimiter;
    private int rateLimitDelayedMs;
    private boolean preferThrottling;
    private boolean drlOptIn;

    /**
     * @hidden
     */
    private boolean isRefresh;

    /**
     * @hidden
     * This is only used by internal, cross-region requests
     */
    private String oboToken;

    protected Request() {}

    /**
     * @hidden
     * @return the timeout in seconds
     */
    public int getTimeoutInternal() {
        return timeoutMs;
    }

    /**
     * @hidden
     * this is public to allow access from Client during refresh
     * @param timeoutMs the request timeout, in milliseconds
     */
    public void setTimeoutInternal(int timeoutMs) {
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
     * @hidden
     * internal use only
     * Sets the namespace to use for the operation.
     *
     * @param namespace the namespace name
     *
     * @since 5.4.10
     */
    protected void setNamespaceInternal(String namespace) {
        this.namespace = namespace;
    }

    /**
     * Returns the namespace to use for the operation.
     *
     * Note: if a namespace is supplied in the table name for the operation,
     * that namespace will override this one.
     *
     * @return the namespace, or null if not set
     */
    public String getNamespace() {
        return namespace;
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
     * @param nanos start nanos of request processing
     */
    public void setStartNanos(long nanos) {
        startNanos = nanos;
    }

    /**
     * @hidden
     * internal use only
     * @return start nanos of request processing
     */
    public long getStartNanos() {
        return startNanos;
    }

    /**
     * @hidden
     * Sets a delay used in rate limiting
     * @param rateLimitDelayedMs delay in ms
     */
    public void setRateLimitDelayedMs(int rateLimitDelayedMs) {
        this.rateLimitDelayedMs = rateLimitDelayedMs;
    }

    /**
     * Get the time the operation was delayed due to rate limiting.
     * Cloud only.
     * If rate limiting is in place, this value will represent the number of
     * milliseconds that the operation was delayed due to rate limiting.
     * If the value is zero, rate limiting did not apply or the operation
     * did not need to wait for rate limiting.
     * @return delay time in milliseconds
     * @since 5.2.25
     */
    public int getRateLimitDelayedMs() {
        return rateLimitDelayedMs;
    }

    /**
     * @hidden
     * internal use only
     * @param value true or false
     */
    public void setIsRefresh(boolean value) {
        isRefresh = value;
    }

    /**
     * @hidden
     * internal use only
     * @return is refresh
     */
    public boolean getIsRefresh() {
        return isRefresh;
    }

    /**
     * Returns the type name of the request. This is used for stats.
     *
     * @return the type name of the request
     */
    public abstract String getTypeName();

    /**
     * @hidden
     * Cloud only
     * If using DRL, return immediate throttling error if the
     * table is currently over its configured throughput limit.
     * Otherwise, allow DRL to delay request processing to match
     * table limits (default).
     * @param preferThrottling if throttling is preferred
     */
    public void setPreferThrottling(boolean preferThrottling) {
        this.preferThrottling = preferThrottling;
    }

    /**
     * @hidden
     * @return true if throttling is preferred
     */
    public boolean getPreferThrottling() {
        return preferThrottling;
    }

    /**
     * @hidden
     * Cloud only
     * Opt-in to using Distributed Rate Limiting (DRL). This setting
     * will eventually be deprecated, as all requests will eventually
     * use DRL unconditionally in the cloud.
     * @param drlOptIn opt in to using DRL in the cloud
     */
    public void setDRLOptIn(boolean drlOptIn) {
        this.drlOptIn = drlOptIn;
    }

    /**
     * @hidden
     * @return true if opted in to using DRL in the cloud
     */
    public boolean getDRLOptIn() {
        return drlOptIn;
    }

    /**
     * @hidden
     * internal use only
     * @param token the on-behalf-of token
     */
    public void setOboTokenInternal(String token) {
        oboToken = token;
    }

    /**
     * @hidden
     * internal use only
     * @return the on-behalf-of token
     */
    public String getOboToken() {
        return oboToken;
    }

    /**
     * @hidden
     * Copy internal fields to another Request object.
     * @param other the Request object to copy to.
     */
    public void copyTo(Request other) {
        other.setTimeoutInternal(this.timeoutMs);
        other.setCheckRequestSize(this.checkRequestSize);
        other.setCompartmentInternal(this.compartment);
        other.setTableNameInternal(this.tableName);
        other.setNamespaceInternal(this.namespace);
        other.setStartNanos(this.startNanos);
        other.setRetryStats(this.retryStats);
        other.setReadRateLimiter(this.readRateLimiter);
        other.setWriteRateLimiter(this.writeRateLimiter);
        other.setRateLimitDelayedMs(this.rateLimitDelayedMs);
        other.setPreferThrottling(this.preferThrottling);
        other.setDRLOptIn(this.drlOptIn);
    }
}
