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
     * @since 5.4
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
     * ReplicaStats represents replica stats information, that includes the
     * timestamp and the corresponding replicaLag information.
     */
    public static class ReplicaStats {
        /**
         * @hidden
         */
        public long time;
        /**
         * @hidden
         */
        public int replicaLag;

        /**
         * Returns the time stamp in milliseconds since the Epoch
         *
         * @return the time stamp
         */
        public long getTime() {
            return time;
        }

        /**
         * Returns the replica lag in milliseconds
         *
         * @return the replica lag
         */
        public int getReplicaLag() {
            return replicaLag;
        }
    }
}
