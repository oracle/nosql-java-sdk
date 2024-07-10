/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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
     * @hidden
     * @return the read units
     */
    public int getReadUnitsInternal() {
        return readUnits;
    }

    /**
     * @hidden
     * @return the read KB
     */
    public int getReadKBInternal() {
        return readKB;
    }

    /**
     * @hidden
     * @return the write KB
     */
    public int getWriteKBInternal() {
        return writeKB;
    }

    /**
     * @hidden
     * @return the write units
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
     * @hidden
     * @param readKB the read KB
     * @return this
     */
    public Result setReadKB(int readKB) {
        this.readKB = readKB;
        return this;
    }

    /**
     * @hidden
     * @param readUnits the read units
     * @return this
     */
    public Result setReadUnits(int readUnits) {
        this.readUnits = readUnits;
        return this;
    }

    /**
     * @hidden
     * @param writeKB the write KB
     * @return this
     */
    public Result setWriteKB(int writeKB) {
        this.writeKB = writeKB;
        return this;
    }

    /**
     * @hidden
     * @param delayMs the delay in milliseconds
     * @return this
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
     * @hidden
     * internal use only
     * @param rs the stats object to use
     */
    public void setRetryStats(RetryStats rs) {
        retryStats = rs;
    }

    /**
     * @hidden
     * @return the current topology info
     */
    public TopologyInfo getTopology() {
        return topology;
    }

    /**
     * @hidden
     * @param ti the current topology info
     */
    public void setTopology(TopologyInfo ti) {
        topology = ti;
    }

    /**
     * @hidden
     * Returns the server protocol serial version or 0 if not available.
     * This is a new feature not supported in older servers.
     *
     * @return the serial version of the server
     */
    public int getServerSerialVersion() {
        return serialVersion;
    }

    /**
     * @hidden
     * @param version the server's serial version
     */
    public void setServerSerialVersion(int version) {
        serialVersion = version;
    }
}
