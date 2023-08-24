/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.util.Collections;
import java.util.Map;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.values.TimestampValue;

/**
 * Cloud service only.
 * <p>
 * ReplicaStatsResult is returned from {@link NoSQLHandle#getReplicaStats}.
 * It contains replica statistics for the requested table.
 * @see NoSQLHandle#getReplicaStats
 *
 * @since 5.4.13
 */
public class ReplicaStatsResult extends Result {
    private String tableName;
    private long nextStartTime;
    private Map<String, ReplicaStats[]> statsRecords;

    /**
     * Returns the table name used by the operation
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the next start time. This can be provided to
     * {@link ReplicaStatsRequest} to be used as a starting point or listing
     * replica stats records.
     *
     * @return the next start time
     */
    public long getNextStartTime() {
        return nextStartTime;
    }

    /**
     * Returns replica statistics information based on the arguments of
     * the {@link ReplicaStatsRequest} used for the request. It will contain
     * stats for either one replica or all replicas.
     *
     * @return a map of replica name to replica stats information collection
     */
    public Map<String, ReplicaStats[]> getStatsRecord() {
        return (statsRecords != null) ? statsRecords : Collections.emptyMap();
    }

    /**
     * @hidden
     * @param tableName the table name
     * @return this
     */
    public ReplicaStatsResult setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * @hidden
     * @param nextStartTime the next startTime
     * @return this
     */
    public ReplicaStatsResult setNextStartTime(long nextStartTime) {
        this.nextStartTime = nextStartTime;
        return this;
    }

    /**
     * @hidden
     * @param records replica stats records
     * @return this
     */
    public ReplicaStatsResult setStatsRecords(
            Map<String, ReplicaStats[]> records) {
        statsRecords = records;
        return this;
    }

    /**
     * ReplicaStats contains information about replica lag for a specific
     * replica.
     *
     * Replica lag is a measure of how current this table is relative to
     * the remote replica and indicates that this table has not yet received
     * updates that happened within the lag period.
     *
     * For example, if the replica lag is 5,000 milliseconds(5 seconds),
     * then this table will have all updates that occurred at the remote
     * replica that are more than 5 seconds old.
     *
     * Replica lag is calculated based on how long it took for the latest
     * operation from the table at the remote replica to be replayed at this
     * table. If there have been no application writes for the table at the
     * remote replica, the service uses other mechanisms to calculate an
     * approximation of the lag, and the lag statistic will still be available.
     */
    public static class ReplicaStats {
        /**
         * @hidden
         */
        public long collectionTimeMillis;

        /**
         * @hidden
         */
        public int replicaLag;

        /**
         * Returns the time the replica lag collection was performed. The value
         * is a time stamp in milliseconds since the Epoch
         *
         * @return the collection time
         */
        public long getCollectionTime() {
            return collectionTimeMillis;
        }

        /**
         * Returns the collection time as an ISO 8601 formatted string
         *
         * @return the collection time string
         */
         public String getCollectionTimeString() {
             return new TimestampValue(collectionTimeMillis).getString();
         }

        /**
         * Returns the replica lag collected at the specified time in
         * milliseconds
         *
         * @return the replica lag
         */
        public int getReplicaLag() {
            return replicaLag;
        }
    }
}
