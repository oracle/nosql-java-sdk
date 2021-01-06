/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.values.TimestampValue;

/**
 * Cloud service only.
 * <p>
 * GetTableUsageResult is returned from {@link NoSQLHandle#getTableUsage}.
 * It encapsulates the dynamic state of the requested table.
 * @see NoSQLHandle#getTableUsage
 */
public class TableUsageResult extends Result {
    private String tableName;
    private TableUsage[] usageRecords;

    /**
     * Returns the table name used by the operation
     *
     * @return the table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns an array of usage records based on the parameters of
     * the {@link TableUsageRequest} used.
     *
     * @return an arry of usage records
     */
    public TableUsage[] getUsageRecords() {
        return usageRecords;
    }

    /**
     * @hidden
     * @param tableName the table name
     * @return this
     */
    public TableUsageResult setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    /**
     * @hidden
     * @param records usage records
     * @return this
     */
    public TableUsageResult setUsageRecords(TableUsage[] records) {
        this.usageRecords = records;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("GetTableUsageResult [table=");
        sb.append(tableName).append("] [tableUsage=[");
        if (usageRecords == null) {
            sb.append("null");
        } else {
            for (int i = 0; i < usageRecords.length; i++) {
                usageRecords[i].toBuilder(sb);
                if (i < (usageRecords.length - 1)) {
                    sb.append(",");
                }
            }
        }
        sb.append("]]");
        return sb.toString();
    }

    /**
     * TableUsage represents a single usage record, or slice, that includes
     * information about read and write throughput consumed during that period
     * as well as the current information regarding storage capacity. In
     * addition the count of throttling exceptions for the period is reported.
     */
    public static class TableUsage {
        /**
         * @hidden
         */
        public long startTimeMillis;
        /**
         * @hidden
         */
        public int secondsInPeriod;
        /**
         * @hidden
         */
        public int readUnits;
        /**
         * @hidden
         */
        public int writeUnits;
        /**
         * @hidden
         */
        public int storageGB;
        /**
         * @hidden
         */
        public int readThrottleCount;
        /**
         * @hidden
         */
        public int writeThrottleCount;
        /**
         * @hidden
         */
        public int storageThrottleCount;

        /**
         * @hidden
         */
        public TableUsage() {}

        /**
         * Returns the start time for this usage record in milliseconds
         * since the Epoch.
         *
         * @return the start time
         */
        public long getStartTime() {
            return startTimeMillis;
        }

       /**
        * Returns the start time as an ISO 8601 formatted string. If the
        * start timestamp is not set, null is returned.
        *
        * @return the start time, or null if not set.
        */
        public String getStartTimeString() {
            if (startTimeMillis == 0) {
                return null;
            }
            return new TimestampValue(startTimeMillis).getString();
        }

        /**
         * Returns the number of seconds in this usage record.
         *
         * @return the number of seconds
         */
        public int getSecondsInPeriod() {
            return secondsInPeriod;
        }

        /**
         * Returns the number of read uits consumed during this period
         *
         * @return the read units
         */
        public int getReadUnits() {
            return readUnits;
        }

        /**
         * Returns the number of write uits consumed during this period
         *
         * @return the write units
         */
        public int getWriteUnits() {
            return writeUnits;
        }

        /**
         * Returns the amount of storage consumed by the table. This information
         * may be out of date as it is not maintained in real time.
         *
         * @return the size in gigabytes
         */
        public int getStorageGB() {
            return storageGB;
        }

        /**
         * Returns the number of read throttling exceptions on this table
         * in the time period.
         *
         * @return the number of throttling exceptions
         */
        public int getReadThrottleCount() {
            return readThrottleCount;
        }

        /**
         * Returns the number of write throttling exceptions on this table
         * in the time period.
         *
         * @return the number of throttling exceptions
         */
        public int getWriteThrottleCount() {
            return writeThrottleCount;
        }

        /**
         * Returns the number of storage throttling exceptions on this table
         * in the time period.
         *
         * @return the number of throttling exceptions
         */
        public int getStorageThrottleCount() {
            return storageThrottleCount;
        }

        /**
         * @hidden
         * Output object state to a StringBuilder
         *
         * @param builder the builder to use
         */
        public void toBuilder(StringBuilder builder) {
            builder.append("TableUsage [startTimeMillis=");
            builder.append(startTimeMillis);
            builder.append(", secondsInPeriod=");
            builder.append(secondsInPeriod);
            builder.append(", readUnits=");
            builder.append(readUnits);
            builder.append(", writeUnits=");
            builder.append(writeUnits);
            builder.append(", storageGB=");
            builder.append(storageGB);
            builder.append(", readThrottleCount=");
            builder.append(readThrottleCount);
            builder.append(", writeThrottleCount=");
            builder.append(writeThrottleCount);
            builder.append(", storageThrottleCount=");
            builder.append(storageThrottleCount);
            builder.append("]");
        }
    }
}
