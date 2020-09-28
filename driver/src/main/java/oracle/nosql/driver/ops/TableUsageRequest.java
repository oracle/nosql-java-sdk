/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
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
 * Represents the argument of a {@link NoSQLHandle#getTableUsage} operation
 * which returns dynamic information associated with a table, as returned in
 * {@link TableUsageResult}. This information includes a time series of
 * usage snapshots, each indicating data such as read and write throughput,
 * throttling events, etc, as found in {@link TableUsageResult.TableUsage}.
 * <p>
 * It is possible to return a range of usage records or, by default, only the
 * most recent usage record. Usage records are created on a regular basis and
 * maintained for a period of time. Only records for time periods that have
 * completed are returned so that a user never sees changing data for a specific
 * range.
 * @see NoSQLHandle#getTableUsage
 */
public class TableUsageRequest extends Request {
    private long startTime;
    private long endTime;
    private int limit;

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
    public TableUsageRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Sets the table name to use for the request. This is a required parameter.
     *
     * @param tableName the table name
     *
     * @return this
     */
    public TableUsageRequest setTableName(String tableName) {
        super.setTableNameInternal(tableName);
        return this;
    }

    /**
     * Sets the start time to use for the request in milliseconds since the
     * Epoch in UTC time. If no time range is set for this request the most
     * recent complete usage record is returned.
     *
     * @param startTime the start time
     *
     * @return this
     */
    public TableUsageRequest setStartTime(long startTime) {
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
     * @return this
     */
    public TableUsageRequest setStartTime(String startTime) {
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
     * Sets the end time to use for the request in milliseconds since the
     * Epoch in UTC time. If no time range is set for this request the most
     * recent complete usage record is returned.
     *
     * @param endTime the end time
     *
     * @return this
     */
    public TableUsageRequest setEndTime(long endTime) {
        if (endTime < 0) {
            throw new IllegalArgumentException("endTime must be >= 0");
        }
        this.endTime = endTime;
        return this;
    }

    /**
     * Sets the end time from an ISO 8601 formatted string. If timezone
     * is not specified it is interpreted as UTC.
     *
     * @param endTime the string of a Timestamp in ISO 8601 format
     * "uuuu-MM-dd['T'HH:mm:ss[.f..f]]".
     * @return this
     */
    public TableUsageRequest setEndTime(String endTime) {
        this.endTime = new TimestampValue(endTime).getLong();
        return this;
    }

    /**
     * Returns the end time to use for the request in milliseconds since
     * the Epoch.
     *
     * @return the end time
     */
    public long getEndTime() {
        return endTime;
    }

    /**
     * Returns the end time as an ISO 8601 formatted string. If the
     * end timestamp is not set, null is returned.
     *
     * @return the end time, or null if not set.
     */
    public String getEndTimeString() {
        if (endTime == 0) {
            return null;
        }
        return new TimestampValue(endTime).getString();
    }

    /**
     * Sets the limit to the number of usage records desired. If this value is
     * 0 there is no limit, but not all usage records may be returned in a
     * single request due to size limitations.
     *
     * @param limit the numeric limit
     *
     * @return this
     */
    public TableUsageRequest setLimit(int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }
        this.limit = limit;
        return this;
    }

    /**
     * Returns the limit to the number of usage records desired.
     *
     * @return the end time
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Sets the request timeout value, in milliseconds. This overrides any
     * default value set in {@link NoSQLHandleConfig}. The value must be
     * positive.
     *
     * @param timeoutMs the timeout value, in milliseconds
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is less than
     * or equal to 0
     */
    public TableUsageRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /**
     * @hidden
     */
    @Override
    public void validate() {
        if (tableName == null) {
            throw new IllegalArgumentException(
                "TableUsageRequest requires a table name");
        }
        if (startTime > 0 && endTime > 0 && endTime < startTime) {
            throw new IllegalArgumentException("TableUsageRequest: " +
                "the endTime must be greater than startTime");
        }
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createGetTableUsageSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createGetTableUsageDeserializer();
    }

    /**
     * @hidden
     */
    @Override
    public boolean shouldRetry() {
        return false;
    }
}
