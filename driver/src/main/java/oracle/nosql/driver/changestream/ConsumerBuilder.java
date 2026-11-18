/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https: *oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.changestream;

import java.util.ArrayList;
import java.util.List;
import java.time.Duration;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.OperationNotSupportedException;
import oracle.nosql.driver.changestream.ConsumerRequest.RequestMode;
import oracle.nosql.driver.http.NoSQLHandleImpl;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.TableResult;

/**
 * ConsumerBuilder represents the builder to use when creating a Consumer.
 * Typically this object is created and populated with API calls rather than creating it
 * directly:
 *
 *    consumer = new ConsumerBuilder()
 *        .addTable("client_info", null, StartLocation.latest())
 *        .groupId("test_group")
 *        .commitAutomatic()
 *        .handle(handle)
 *        .build()
 */
public class ConsumerBuilder {

    /*
     * TableConfig represents the details for a single table in a change
     * streaming configuration. It is typically created using API calls:
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

        /* Optional start location. If null, EARLIEST is used as the default. */
        public StartLocation startLocation;

        /* Table Ocid. One of tableName or tableOcid are required. */
        public String tableOcid;

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
                this.startLocation = StartLocation.earliest();
            } else {
                this.startLocation = startLocation;
            }
        }
    }


    /* The single table to consume from, represented as a list for the protocol. */
    public List<TableConfig> tables;

    /*
     * The group ID. In NoSQL Change Streaming, every consumer is part of a "group".
     * The group may be a single consumer, or may have multiple consumers.
     *
     * When multiple consumers use the same group ID, the NoSQL system will attempt
     * to evenly distribute data for the specified table across all consumers in
     * the group. Data to any one consumer will be consistently ordered for records using
     * the same shard key. That is, different consumers will not get data for the same shard key.
     * Any group ID used should be sufficiently unique to avoid unintended alterations to
     * existing groups.
     *
     * If there is only one consumer in a group, this consumer will
     * always get all of the data for the table specified in the group.
     *
     * Each consumer group consumes exactly one table. All consumers in the
     * group must specify the same table.
     *
     * If a table is already being consumed by other consumers in this group,
     * this consumer starts from the group's existing position (the start
     * location specified in the config is ignored). If a table is not in the
     * existing group (or if this the first consumer in this group), the
     * StartLocation in the table config will be used.
     * This behavior can be changed by setting forceResetStartLocation() to true in the config.
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
     * Background keepalive polls count toward this interval.
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
     * Specifies the single table for the consumer group.
     * Only one table may be configured per group.
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
     * in this group, this consumer starts from the group's existing position
     * (the start location specified here is ignored). If a table is not in
     * the existing group (or if this the first consumer in this group), the
     * startLocation specified here will be used.
     *
     * If location is null, StartLocation.EARLIEST is the default.
     *
     * @throws IllegalArgumentException if a table is already specified
     */
    public ConsumerBuilder addTable(String tableName,
                                    String compartmentOcid,
                                    StartLocation location) {
        if (tables != null && !tables.isEmpty()) {
            throw new IllegalArgumentException(
                "Only one table may be specified per consumer group");
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

    /**
     * Specify the group ID.
     * In NoSQL Change Streaming, every consumer is part of a "group".
     * The group may have a single consumer, or may have multiple consumers.
     *
     * When there is only a single consumer in a group, this consumer will
     * always get all of the data for the table specified in the group.
     *
     * When multiple consumers use the same group ID, the NoSQL system will attempt
     * to evenly distribute data for the specified table across all consumers in
     * the group. Data to any one consumer will be consistently ordered for records using
     * the same shard key. That is, different consumers will not get data for the same shard key.
     * Any group ID used should be sufficiently unique to avoid unintended alterations to
     * existing groups.
     *
     * Each consumer group consumes exactly one table. All consumers in the
     * group must specify the same table.
     *
     * If a table is already being consumed by other consumers in this group,
     * this consumer starts from the group's existing position (the start
     * location specified in the config is ignored). If a table is not in the
     * existing group (or if this the first consumer in this group), the
     * StartLocation in the table config will be used.
     * This behavior can be changed by specifying forceResetStartLocation() in the builder.
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
     * This setting allows the application to determine what will happen if a
     * consumer process dies unexpectedly, or as part of a restart operation.
     * If a consumer process dies, the data that it would be consuming
     * will not be consumed by any other consumers in this consumer's group until
     * this interval expires. The consumer sends background keepalive polls at
     * half this interval when the application is not polling, with a minimum
     * background poll interval of one second.
     *
     * If not specified, the default value for maxPollInterval is 30 seconds.
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

    /**
     * Set the cloud compartment ID to use for this consumer.
     * This is optional. If no compartment is specified, the default
     * (root) compartment of the tenancy is used.
     * The table may be in a different compartment, as specified in addTable().
     *
     * @param compartmentOcid The OCID of the compartment to use
     */
    public ConsumerBuilder compartmentOcid(String compartmentOcid) {
        this.compartmentOcid = compartmentOcid;
        return this;
    }

    /**
     * Create a Change Streaming consumer based on builder configuration.
     *
     * This will make server-side calls to validate all configuration and
     * establish server-side state for the consumer.
     *
     * Rebalancing begins with the first application or background keepalive poll.
     */
    public Consumer build() {
        return new Consumer(this);
    }

    /**
     * Get the minimum number of consumers required to completely read all
     * events in a stream within a specified timeframe.
     *
     * Given the Builder configuration (groupID, table, start mode),
     * this method will return the minimum number of consumers needed to
     * read all events within the duration given.
     * The Builder config must have either a valid current groupID, or
     * a table name, compartmentId, and startMode.
     *
     * Note the returned value is an estimate: application-side
     * processing time of events may affect the accuracy of this value.
     *
     * @param timeframe The desired time to read all events
     * @return the number of consumers needed to read all events in
     * the given timeframe.
     */
    public int getMinimumConsumers(Duration timeframe) {
        if (this.handle == null) {
            throw new IllegalArgumentException(
                "getMinimumConsumers requires a valid NoSQL handle");
        }
        ConsumerRequest req = new ConsumerRequest(RequestMode.MIN_CONSUMERS).
                                     setBuilder(this).timeframe(timeframe);
        try {
            ConsumerResult res =
                (ConsumerResult) ((NoSQLHandleImpl)this.handle).getClient().execute(req);
            return res.minimumConsumers;
        } catch (Exception e) {
            if (e.getMessage().contains("unknown opcode")) {
                throw new OperationNotSupportedException("Change Streaming not supported by server");
            }
            throw e;
        }
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
