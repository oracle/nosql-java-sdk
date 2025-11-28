/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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
    private volatile long startNanos;

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

    protected int topoSeqNum = -1;

    /**
     * @hidden
     * This is only required by Java SDK for internal cross-region request, not
     * by other drivers.
     */
    private String oboToken;

    /**
     * Construct a request
     * @hidden
     */
    protected Request() {}

    /**
     * Returns timeout
     * @return the timeout in seconds
     * @hidden
     */
    public int getTimeoutInternal() {
        return timeoutMs;
    }

    /**
     * this is public to allow access from Client during refresh
     * @param timeoutMs the request timeout, in milliseconds
     * @hidden
     */
    public void setTimeoutInternal(int timeoutMs) {
        if (timeoutMs <= 0) {
            throw new IllegalArgumentException("timeout must be > 0");
        }
        this.timeoutMs = timeoutMs;
    }

    /**
     * Internal use only.
     *
     * Sets default values in a request based on the specified config
     * object. This will typically be overridden by subclasses.
     *
     * @param config the configuration object to use to get default values
     *
     * @return this
     * @hidden
     */
    public Request setDefaults(NoSQLHandleConfig config) {
        if (timeoutMs == 0) {
            timeoutMs = config.getDefaultRequestTimeout();
        }
        return this;
    }

    /**
     * Return if this request should be retried.
     *
     * @return true if the request should be retried
     * @hidden
     */
    public boolean shouldRetry() {
        return true;
    }

    /**
     * is the request a query?
     * @return true if the request is a query
     * @hidden
     */
    public boolean isQueryRequest() {
        return false;
    }

    /**
     * does reads?
     * @return true if the request expects to do reads (incur read units)
     * @hidden
     */
    public boolean doesReads() {
        return false;
    }

    /**
     * does writes?
     * @return true if the request expects to do writes (incur write units)
     * @hidden
     */
    public boolean doesWrites() {
        return false;
    }

    /**
     * Internal use only
     * Sets the compartment id or name to use for the operation.
     *
     * @param compartment the compartment id
     * @hidden
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
     * internal use only
     * Sets the table name to use for the operation.
     *
     * @param tableName the table name
     * @hidden
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
     * internal use only
     * Sets the namespace to use for the operation.
     *
     * @param namespace the namespace name
     *
     * @since 5.4.10
     * @hidden
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
     * @hidden
     */
    public abstract void validate();

    /**
     * Internal use only
     *
     * Returns an object that can serialize this request type.
     *
     * @param factory a factory instance used to construct the serializer
     *
     * @return an object used to serialize this request
     * @hidden
     */
    public abstract Serializer createSerializer(
        SerializerFactory factory);

    /**
     * Internal use only
     *
     * Returns an object that can serialize this request type.
     *
     * @param factory a factory instance used to construct the serializer
     *
     * @return an object used to de-serialize this request
     * @hidden
     */
    public abstract Serializer createDeserializer(
        SerializerFactory factory);

    /**
     * request size
     * @return the value
     * @hidden
     */
    public boolean getCheckRequestSize() {
        return checkRequestSize;
    }

    /**
     * value size
     * @param value the value
     * @return the value
     * @hidden
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
     * internal use only
     * This is typically set by internal request processing when the
     * first retry is attempted. It is used/updated thereafter on
     * subsequent retry attempts.
     * @param rs the stats object to use
     * @hidden
     */
    public void setRetryStats(RetryStats rs) {
        retryStats = rs;
    }

    /**
     * internal use only
     * This adds (or increments) a class type to the list of exceptions
     * that were processed during retries of a single request operation.
     * @param re class of exception to add to retry stats
     * @hidden
     */
    public void addRetryException(Class<? extends Throwable> re) {
        if (retryStats == null) {
            retryStats = new RetryStats();
        }
        retryStats.addException(re);
    }

    /**
     * internal use only
     * This adds time to the total time spent processing retries during
     * a single request processing operation.
     * @param millis time to add to retry delay value
     * @hidden
     */
    public void addRetryDelayMs(int millis) {
        if (retryStats == null) {
            retryStats = new RetryStats();
        }
        retryStats.addDelayMs(millis);
    }

    /**
     * internal use only
     * @return time spent in retries, in milliseconds
     * @hidden
     */
    public int getRetryDelayMs() {
        if (retryStats == null) {
            return 0;
        }
        return retryStats.getDelayMs();
    }

    /**
     * internal use only
     * Increments the number of retries during the request operation.
     * @hidden
     */
    public void incrementRetries() {
        if (retryStats == null) {
            retryStats = new RetryStats();
        }
        retryStats.incrementRetries();
    }

    /**
     * internal use only
     * @return number of retries
     * @hidden
     */
    public int getNumRetries() {
        if (retryStats == null) {
            return 0;
        }
        return retryStats.getRetries();
    }

    /**
     * internal use only
     * @param nanos start nanos of request processing
     * @hidden
     */
    public void setStartNanos(long nanos) {
        startNanos = nanos;
    }

    /**
     * internal use only
     * @return start nanos of request processing
     * @hidden
     */
    public long getStartNanos() {
        return startNanos;
    }

    /**
     * Sets a delay used in rate limiting
     * @param rateLimitDelayedMs delay in ms
     * @hidden
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
     * internal use only
     * @param value true or false
     * @hidden
     */
    public void setIsRefresh(boolean value) {
        isRefresh = value;
    }

    /**
     * internal use only
     * @return is refresh
     * @hidden
     */
    public boolean getIsRefresh() {
        return isRefresh;
    }

    /**
     * internal use only
     * @return the topo sequence number
     * @hidden
     */
    public int topoSeqNum() {
        return topoSeqNum;
    }

    /**
     * internal use only
     * @param n the topo sequence number
     * @hidden
     */
    public void setTopoSeqNum(int n) {
        if (topoSeqNum < 0) {
            topoSeqNum = n;
        }
    }

    /**
     * Returns the type name of the request. This is used for stats.
     *
     * @return the type name of the request
     */
    public abstract String getTypeName();

    /**
     * Cloud only
     * If using DRL, return immediate throttling error if the
     * table is currently over its configured throughput limit.
     * Otherwise, allow DRL to delay request processing to match
     * table limits (default).
     * @param preferThrottling if throttling is preferred
     * @hidden
     */
    public void setPreferThrottling(boolean preferThrottling) {
        this.preferThrottling = preferThrottling;
    }

    /**
     * internal use only
     * @return true if throttling is preferred
     * @hidden
     */
    public boolean getPreferThrottling() {
        return preferThrottling;
    }

    /**
     * Cloud only
     * Opt-in to using Distributed Rate Limiting (DRL). This setting
     * will eventually be deprecated, as all requests will eventually
     * use DRL unconditionally in the cloud.
     * @param drlOptIn opt in to using DRL in the cloud
     * @hidden
     */
    public void setDRLOptIn(boolean drlOptIn) {
        this.drlOptIn = drlOptIn;
    }

    /**
     * internal use only
     * @return true if opted in to using DRL in the cloud
     * @hidden
     */
    public boolean getDRLOptIn() {
        return drlOptIn;
    }

    /**
     * This is only required by Java SDK for internal cross-region request, not
     * by other drivers.
     *
     * @param token the on-behalf-of token
     * @hidden
     */
    public void setOboTokenInternal(String token) {
        oboToken = token;
    }

    /**
     * This is only required by Java SDK for internal cross-region request, not
     * by other drivers.
     *
     * @return the on-behalf-of token
     * @hidden
     */
    public String getOboToken() {
        return oboToken;
    }

    /**
     * Copy internal fields to another Request object.
     * Use direct member assignment to avoid value checks that only apply
     * to user-based assignments.
     * @param other the Request object to copy to.
     * @hidden
     */
    public void copyTo(Request other) {
        other.timeoutMs = this.timeoutMs;
        other.checkRequestSize = this.checkRequestSize;
        other.compartment = this.compartment;
        other.tableName = this.tableName;
        other.namespace = this.namespace;
        other.startNanos = this.startNanos;
        other.retryStats = this.retryStats;
        other.readRateLimiter = this.readRateLimiter;
        other.writeRateLimiter = this.writeRateLimiter;
        other.rateLimitDelayedMs = this.rateLimitDelayedMs;
        other.preferThrottling = this.preferThrottling;
        other.drlOptIn = this.drlOptIn;
    }
}
