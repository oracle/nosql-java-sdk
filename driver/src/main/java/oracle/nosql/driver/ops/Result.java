/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.query.TopologyInfo;

/**
 * Result is a base class for result classes for all supported operations.
 * All state and methods are maintained by extending classes.
 */
public class Result {
    /*
     * readUnits and readKB will be different in the case of Absolute
     * Consistency. writeUnits and writeKB will always be equal.
     */
    private int readKB;
    private int readUnits;
    private int writeKB;

    /*
     * if available, the serial version of the proxy, otherwise 0. This
     * allows the SDK to conditionalize code and tests based on new features
     * and semantics
     */
    private int serialVersion;

    /*
     * Cloud Only
     * If rate limiting is in place, this value will represent the number of
     * milliseconds that the operation was delayed due to rate limiting.
     * If the value is zero, rate limiting did not apply or the operation
     * did not need to wait for rate limiting.
     */
    private int rateLimitDelayedMs;

    /**
     * @hidden
     * This is copied over from the Request object when an operation
     * is successful.
     */
    private RetryStats retryStats;

    private TopologyInfo topology;

    protected Result() {}

    /**
     * internal use only
     * @return the read units
     * @hidden
     */
    public int getReadUnitsInternal() {
        return readUnits;
    }

    /**
     * internal use only
     * @return the read KB
     * @hidden
     */
    public int getReadKBInternal() {
        return readKB;
    }

    /**
     * internal use only
     * @return the write KB
     * @hidden
     */
    public int getWriteKBInternal() {
        return writeKB;
    }

    /**
     * internal use only
     * @return the write units
     * @hidden
     */
    public int getWriteUnitsInternal() {
        return writeKB;
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
     * @param readKB the read KB
     * @return this
     * @hidden
     */
    public Result setReadKB(int readKB) {
        this.readKB = readKB;
        return this;
    }

    /**
     * internal use only
     * @param readUnits the read units
     * @return this
     * @hidden
     */
    public Result setReadUnits(int readUnits) {
        this.readUnits = readUnits;
        return this;
    }

    /**
     * internal use only
     * @param writeKB the write KB
     * @return this
     * @hidden
     */
    public Result setWriteKB(int writeKB) {
        this.writeKB = writeKB;
        return this;
    }

    /**
     * internal use only
     * @param delayMs the delay in milliseconds
     * @return this
     * @hidden
     */
    public Result setRateLimitDelayedMs(int delayMs) {
        this.rateLimitDelayedMs = delayMs;
        return this;
    }


    /**
     * Returns a stats object with information about retries.
     *
     * @return stats object with retry information, or null if
     *         no retries were performed.
     */
    public RetryStats getRetryStats() {
        return retryStats;
    }

    /**
     * internal use only
     * @param rs the stats object to use
     * @hidden
     */
    public void setRetryStats(RetryStats rs) {
        retryStats = rs;
    }

    /**
     * internal use only
     * @return the current topology info
     * @hidden
     */
    public TopologyInfo getTopology() {
        return topology;
    }

    /**
     * internal use only
     * @param ti the current topology info
     * @hidden
     */
    public void setTopology(TopologyInfo ti) {
        topology = ti;
    }

    /**
     * Returns the server protocol serial version or 0 if not available.
     * This is a new feature not supported in older servers.
     *
     * @return the serial version of the server
     * @hidden
     */
    public int getServerSerialVersion() {
        return serialVersion;
    }

    /**
     * internal use only
     * @param version the server's serial version
     * @hidden
     */
    public void setServerSerialVersion(int version) {
        serialVersion = version;
    }
}
