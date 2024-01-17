/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.values.TimestampValue;

/**
 * Cloud service only.
 * <p>
 * Represents the argument of a {@link NoSQLHandle#getReplicaStats} operation
 * which returns stats information for one, or all replicas of a replicated
 * table, returned in {@link ReplicaStatsResult}. This information includes a
 * time series of replica stats, as found in
 * {@link ReplicaStatsResult.ReplicaStats}.
 * <p>
 * It is possible to return a range of stats records or, by default, only the
 * most recent stats records if startTime is not specified. Replica stats
 * records are created on a regular basis and maintained for a period of time.
 * Only records for time periods that have completed are returned so that a user
 * never sees changing data for a specific range.
 * @see NoSQLHandle#getReplicaStats
 *
 * @since 5.4.13
 */
public class ReplicaStatsRequest extends Request {

    private String replicaName;
    private long startTime;
    private int limit;

    /**
     * Sets the table name to use for the operation.
     *
     * @param tableName the name
     *
     * @return this
     */
    public ReplicaStatsRequest setTableName(String tableName) {
        super.setTableNameInternal(tableName);
        return this;
    }

    /**
     * Sets the replica name to query the stats information. If not set,
     * information for all replicas is returned.
     *
     * @param replicaName the replica name
     *
     * @return this
     */
    public ReplicaStatsRequest setReplicaName(String replicaName) {
        this.replicaName = replicaName;
        return this;
    }

    /**
     * Gets the replica name to query the stats information or null if
     * not set
     *
     * @return replica name
     */
    public String getReplicaName() {
        return replicaName;
    }

    /**
     * Sets the start time to use for the request in milliseconds since the
     * Epoch in UTC time. If no start time is set for this request the most
     * recent complete stats records are returned, the number of records is
     * up to limit {@link ReplicaStatsRequest#setLimit}
     *
     * @param startTime the start time
     *
     * @return this
     */
    public ReplicaStatsRequest setStartTime(long startTime) {
        if (startTime < 0) {
            throw new IllegalArgumentException("startTime must be >= 0");
        }
        this.startTime = startTime;
        return this;
    }

    /**
     * Sets the start time from an ISO 8601 formatted string. If timezone
     * is not specified it is interpreted as UTC.
     *
     * @param startTime the string of a Timestamp in ISO 8601 format
     * "uuuu-MM-dd['T'HH:mm:ss[.f..f]]".
     *
     * @return this
     *
     * @throws IllegalArgumentException if the startTime string is not in valid
     * format.
     */
    public ReplicaStatsRequest setStartTime(String startTime) {
        this.startTime = new TimestampValue(startTime).getLong();
        return this;
    }

    /**
     * Returns the start time to use for the request in milliseconds since
     * the Epoch.
     *
     * @return the start time
     */
    public long getStartTime() {
        return startTime;
    }

    /**
     * Returns the start time as an ISO 8601 formatted string. If the
     * start timestamp is not set, null is returned.
     *
     * @return the start time, or null if not set.
     */
    public String getStartTimeString() {
        if (startTime == 0) {
            return null;
        }
        return new TimestampValue(startTime).getString();
    }

    /**
     * Sets the limit to the number of replica stats records per region, the
     * default value is 1000.
     *
     * @param limit the numeric limit
     *
     * @return this
     */
    public ReplicaStatsRequest setLimit(int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }
        this.limit = limit;
        return this;
    }

    /**
     * Returns the limit to the number of replica stats records desired.
     *
     * @return the end time
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Cloud service only.
     * <p>
     * Sets the name or id of a compartment to be used for this operation.
     * <p>
     * The compartment may be specified as either a name (or path for nested
     * compartments) or as an id (OCID). A name (vs id) can only
     * be used when authenticated using a specific user identity. It is
     * <b>not</b> available if authenticated as an Instance Principal which can
     * be done when calling the service from a compute instance in the Oracle
     * Cloud Infrastructure.  See {@link
     * SignatureProvider#createWithInstancePrincipal}
     *
     * @param compartment the name or id. If using a nested compartment,
     * specify the full compartment path
     * <code>compartmentA.compartmentB</code>, but exclude the name of the
     * root compartment (tenant).
     *
     * @return this
     */
    public ReplicaStatsRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Sets the request timeout value, in milliseconds. This overrides any
     * default value set with {@link NoSQLHandleConfig#setRequestTimeout}.
     * The value must be positive.
     *
     * @param timeoutMs the timeout value, in milliseconds
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is less than
     * or equal to 0
     */
    public ReplicaStatsRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    @Override
    public void validate() {
        if (tableName == null) {
            throw new IllegalArgumentException(
                "ReplicaStatsRequest requires table name");
        }
    }

    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createGetReplicaStatsSerializer();
    }

    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createGetReplicaStatsDeserializer();
    }

    @Override
    public String getTypeName() {
        return "ReplicaStats";
    }

    /**
     * @hidden
     */
    @Override
    public boolean shouldRetry() {
        return false;
    }
}
