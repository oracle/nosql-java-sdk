/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https: *oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.changestream;

import java.util.ArrayList;
import java.util.List;
import java.time.Duration;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.TableResult;

/**
 * ConsumerBuilder represents the builder to use when creating a Consumer.
 * Typically this object is created and populated with API calls rather than creating it
 * directly:
 *
 *    consumer = new ConsumerBuilder()
 *        .addTable("client_info", null, StartLocation.latest())
 *        .addTable("location_data", null, StartLocation.latest())
 *        .groupId("test_group")
 *        .commitAutomatic()
 *        .handle(handle)
 *        .build()
 */
public class ConsumerBuilder {

    /*
     * TableConfig represents the details for a single table in a change
     * data capture configuration. It is typically created using API calls:
     *
     *    config = new ConsumerBuilder().
     *        addTable("client_info", null, StartLocation.latest());
     */
    public class TableConfig {
        /* Name of the table. One of tableName or tableOcid are required. */
        public String tableName;

        /* Optional compartment ID for the table. If empty, the default compartment */
        /* for the tenancy is used. */
        public String compartmentOcid;

        /* Optional start location. If null, FIRST_UNCOMMITTED is used as the default. */
        public StartLocation startLocation;

        /* Table Ocid. One of tableName or tableOcid are required. */
        public String tableOcid;

        /* if this is set, this table should be removed instead of added */
        public boolean isRemove;

        TableConfig(String tableName,
                    String compartmentOcid,
                    StartLocation startLocation) {
            if (tableName == null) {
                throw new IllegalArgumentException("table name is required");
            }
            if (tableName.startsWith("ocid1.nosqltable.")) {
                this.tableOcid = tableName;
            } else {
                this.tableName = tableName;
            }
            this.compartmentOcid = compartmentOcid;
            if (startLocation == null) {
                this.startLocation = StartLocation.firstUncommitted();
            } else {
                this.startLocation = startLocation;
            }
        }
    }


    /* Tables to consume from. This list must have at least one table config defined. */
    public List<TableConfig> tables;

    /*
     * The group ID. In NoSQL Change Data Capture, every consumer is part of a "group".
     * The group may be a single consumer, or may have multiple consumers.
     *
     * When multiple consumers use the same group ID, the NoSQL system will attempt
     * to evenly distribute data for the specified tables evenly across all consumers in
     * the group. Data to any one consumer will be consistently ordered for records using
     * the same shard key. That is, different consumers will not get data for the same shard key.
     * Any group ID used should be sufficiently unique to avoid unintended alterations to
     * existing groups.
     *
     * If there is only one consumer in a group, this consumer will
     * always get all of the data for all of the tables specified in the group.
     *
     * NOTE: The Tables in this config will override any previous tables that other
     * existing consumers in this group have specified. Any tables that are being
     * currently consumed by this group that are not in the specified Tables list will
     * have their consumption stopped.
     *
     * If a table is already being consumed by other consumers in this group, this
     * consumer's start location for the table will be FirstUncommitted (the start location
     * specified in the config is ignored). If a table is not in the existing group (or if this the
     * first consumer in this group), the StartLocation in the table config will be used.
     * This behavior can be changed by setting ForceReset to true in the config.
     */
    public String groupId;

    /*
     * The compartment ID to use for the consumer group. If this is empty, the
     * default tenancy ID will be used.
     */
    public String compartmentOcid;

    /*
     * Specify the commit mode for the consumer. If this value is true, the system will not
     * automatically commit messages consumed by poll(). It is the responsibility of the
     * application to call Commit() on a timely basis after consumed data has been processed.
     * If this value is false (the default), commits will be done automatically: every call
     * to poll() will automatically mark the data returned by the previous poll() as committed.
     */
    public boolean manualCommit;

    /*
     * Specify the maximum interval between calls to poll() before the system will
     * consider this consumer as failed, which will trigger a rebalance operation to
     * redirect its change event data to other active consumers.
     */
    public Duration maxPollInterval;

    /*
     * Force resetting the start location for the consumer(s) in the group. This is typically
     * only used when a consumer group is completely stopped and a new group with the same
     * group ID is to be started at a given start location (Earliest, Latest, etc).
     *
     * This setting will remove any existing consumers' committed locations.
     *
     * NOTE: Usage of this setting is dangerous, as any currently running consumers in the group
     * will have their current and committed locations reset unexpectedly.
     */
    public boolean forceReset;

    /*
     * The NoSQL handle to use for all consumer operations.
     */
    public NoSQLHandle handle;

    public ConsumerBuilder() {}

    private int tableIndex(String tableName, String compartmentOcid) {
        if (tables == null) {
            return -1;
        }
        int x = 0;
        for (TableConfig tcfg : tables) {
            if (tcfg.tableName.equalsIgnoreCase(tableName) ||
                (tcfg.tableOcid != null &&
                 tcfg.tableOcid.equalsIgnoreCase(tableName))) {
                if (compartmentOcid == null) {
                    if (tcfg.compartmentOcid == null) {
                        return x;
                    }
                } else {
                    if (tcfg.compartmentOcid != null &&
                        tcfg.compartmentOcid.equalsIgnoreCase(compartmentOcid)) {
                        return x;
                    }
                }
            }
            x++;
        }
        return -1;
    }

    /**
     * Specify the NoSQL handle to use for all consumer operations.
     *
     * Any specific Consumer instance must use one and only one NoSQL Handle.
     */
    public ConsumerBuilder handle(NoSQLHandle handle) {
        this.handle = handle;
        return this;
    }

    /**
     * Adds a table to the consumer config.
     *
     * The table must have already have Change Streaming enabled via the OCI console or
     * a NoSQL SDK {@link NoSQLHandle#enableChangeStreaming} request.
     *
     * tableName: required. This may be the Ocid of the table, if available.
     *
     * compartmentOcid: This is optional. If null, the default compartment Ocid
     * for the tenancy is used.
     *
     * location: Specify the position of the first element to read in the
     * change stream. If a table is already being consumed by other consumers
     * in this group, this consumer's start location for the table will be
     * FIRST_UNCOMMITTED (the start location specified here is ignored). If
     * a table is not in the existing group (or if this the first consumer
     * in this group), the startLocation specified here will be used.
     *
     * If location is null, StartLocation.FIRST_UNCOMMITTED is the default.
     */
    public ConsumerBuilder addTable(String tableName,
                                    String compartmentOcid,
                                    StartLocation location) {
        /* check if table already in config */
        if (tableIndex(tableName, compartmentOcid) >= 0) {
            return this;
        }
        TableConfig tc = new TableConfig(tableName,
                                         compartmentOcid,
                                         location);
        if (tables == null) {
            tables = new ArrayList<TableConfig>();
        }
        tables.add(tc);
        return this;
    }

    /*
     * @hidden
     * Removes a table from the consumer config.
     * This is used internally when removing a table from
     * an existing consumer.
     *
     * tableName: required. This may be the Ocid of the table, if available.
     *
     * compartmentOcid: This is optional. If null, the default compartment Ocid
     * for the tenancy is used.
     */
    public ConsumerBuilder removeTable(String tableName,
                                       String compartmentOcid) {
        TableConfig tc = new TableConfig(tableName, compartmentOcid, null);
        tc.isRemove = true;
        if (tables == null) {
            tables = new ArrayList<TableConfig>();
        }
        tables.add(tc);
        return this;
    }

    /**
     * Specify the group ID.
     * In NoSQL Change Data Capture, every consumer is part of a "group".
     * The group may have a single consumer, or may have multiple consumers.
     *
     * When there is only a single consumer in a group, this consumer will
     * always get all of the data for all of the tables specified in the group.
     *
     * When multiple consumers use the same group ID, the NoSQL system will attempt
     * to evenly distribute data for the specified tables evenly across all consumers in
     * the group. Data to any one consumer will be consistently ordered for records using
     * the same shard key. That is, different consumers will not get data for the same shard key.
     * Any group ID used should be sufficiently unique to avoid unintended alterations to
     * existing groups.
     *
     * NOTE: The Tables in this config will override any previous tables that other
     * existing consumers in this group have specified. Any tables that are being
     * currently consumed by this group that are not in the specified Tables list will
     * have their consumption stopped: calls to [Consumer.poll] from existing consumers will
     * no longer contain data for tables not specified in this config.
     *
     * If a table is already being consumed by other consumers in this group, this
     * consumer's start location for the table will be FirstUncommitted (the start location
     * specified in the config is ignored). If a table is not in the existing group (or if this the
     * first consumer in this group), the StartLocation in the table config will be used.
     * This behavior can be changed by specifying ResetStartLocation() in the config.
     */
    public ConsumerBuilder groupId(String groupId) {
        this.groupId = groupId;
        return this;
    }

    /**
     * Specify automatic commit mode for the consumer. This is the default if not specified.
     * In this mode, commits will be done automatically: every call
     * to Consumer.poll() will automatically mark the data returned by the previous
     * call to Consumer.poll() as committed.
     */
    public ConsumerBuilder commitAutomatic() {
        this.manualCommit = false;
        return this;
    }

    /**
     * Specify manual commit mode for the consumer. The system will not
     * automatically commit messages consumed by Consumer.poll(). It is
     * the responsibility of the application to call Consumer.commit()
     * on a timely basis after consumed data has been processed.
     */
    public ConsumerBuilder commitManual() {
        this.manualCommit = true;
        return this;
    }

    /**
     * Specify the maximum poll interval.
     *
     * This determines the maximum interval between calls to poll() before the system will
     * consider this consumer as failed, which will trigger a rebalance operation to
     * redirect its change event data to other active consumers.
     *
     * If not specified, the default value for maxPollInterval is 30 seconds.
     *
     * Note: if a consumer process dies, the data that it would be consuming
     * will not be consumed by any other consumers in this consumer's group until
     * this interval expires.
     */
    public ConsumerBuilder maxPollInterval(Duration interval) {
        this.maxPollInterval = interval;
        return this;
    }

    /**
     * Force resetting the start location for the consumer(s) in the group. This is typically
     * only used when a consumer group is completely stopped and a new group with the same
     * group ID is to be started at a given start location (Earliest, Latest, etc).
     *
     *    NOTE: Usage of this setting is dangerous, as any currently running consumers in the group
     *          will have their current and committed locations reset unexpectedly.
     *
     * This setting will remove any existing consumers' committed locations.
     */
    public ConsumerBuilder forceResetStartLocation() {
        this.forceReset = true;
        return this;
    }


    /*
     * Validate all tables in the config. This will also populate the tableOcid
     * for each table, which is used internally for all accesses.
     */
    public void validate() {
        if (handle == null) {
            throw new IllegalArgumentException("Consumer builder missing NoSQLHandle");
        }
        if (tables == null || tables.isEmpty()) {
            throw new IllegalArgumentException("Consumer builder missing tables information");
        }
        for (TableConfig tcfg : tables) {
            validateTableConfig(tcfg, handle);
        }
    }

    public static void validateTableConfig(TableConfig tcfg, NoSQLHandle handle) {
        if (tcfg.tableOcid != null) {
            /* TODO: verify OCID format */
            return;
        }
        if (tcfg.tableName == null || tcfg.tableName.isEmpty()) {
            throw new IllegalArgumentException("missing table name in consumer configuration");
        }
        GetTableRequest req = new GetTableRequest().setTableName(tcfg.tableName);
        if (tcfg.compartmentOcid != null) {
            req.setCompartment(tcfg.compartmentOcid);
        }
        try {
            TableResult res = handle.getTable(req);
            tcfg.tableOcid = res.getTableId();
System.out.println("Using ocid='" + tcfg.tableOcid + "' for table='" + tcfg.tableName + "'");
        } catch (Exception e) {
            throw new IllegalArgumentException("Can't get table '" +
                tcfg.tableName + "' information: " + e);
        }
    }

    public ConsumerBuilder setCompartmentOcid(String compartmentOcid) {
        this.compartmentOcid = compartmentOcid;
        return this;
    }

    /**
     * Create a Change Data Capture consumer based on builder configuration.
     *
     * This will make server-side calls to validate all configuration and
     * establish server-side state for the consumer.
     *
     * Any table changes (added tables, removed tables, start locations) for
     * the consumer group will be applied immediately after this call succeeds.
     * It is not necessary to call poll() to trigger table changes.
     *
     * Note that rebalancing operations will not take effect after this call.
     * Rebalancing does not happen until the first call to poll().
     */
    public Consumer build() {
        return new Consumer(this);
    }

    /*
     * @hidden
     */
    public int getNumTables() {
        if (tables == null) {
            return 0;
        }
        return tables.size();
    }
}
