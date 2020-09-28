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

/**
 * TableRequest is used to create, modify, and drop tables. The operations
 * allowed are those supported by the Data Definition Language (DDL) portion of
 * the query language. The language provides for table creation and removal
 * (drop), index add and drop, as well as schema evolution via alter
 * table. Operations using DDL statements infer the table name from the query
 * statement itself, e.g. "create table mytable(...)". Table creation requires
 * a valid {@link TableLimits} object to define the throughput desired for the
 * table. If TableLimits is provided with any other type of query statement an
 * exception is thrown.
 * <p>
 * This request is also used to modify the limits of throughput and storage for
 * an existing table. This case is handled by specifying a table name and
 * limits without a query statement. If all three are specified it is an error.
 * <p>
 * Execution of operations specified by this request is implicitly asynchronous.
 * These are potentially long-running operations.
 * {@link NoSQLHandle#tableRequest} returns a {@link TableResult} instance that
 * can be used to poll until the table reaches the desired state.
 *
 * @see NoSQLHandle#tableRequest
 */
public class TableRequest extends Request {
    private String statement;
    private TableLimits limits;
    /* required for limits-only change; not used otherwise */

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
    public TableRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Returns the statement, or null if not set
     *
     * @return the statement
     */
    public String getStatement() {
        return statement;
    }

    /**
     * Returns the table limits, or null if not set
     *
     * @return the limits
     */
    public TableLimits getTableLimits() {
        return limits;
    }

    /**
     * Sets the query statement to use for the operation. This parameter is
     * required unless the operation is intended to change the limits of an
     * existing table.
     *
     * @param statement the statement
     *
     * @return this
     */
    public TableRequest setStatement(String statement) {
        this.statement = statement;
        return this;
    }

    /**
     * Sets the table name to use for the operation. The table name is only
     * used to modify the limits of an existing table, and must not be set
     * for any other operation.
     *
     * @param tableName the name
     *
     * @return this
     */
    public TableRequest setTableName(String tableName) {
        super.setTableNameInternal(tableName);
        return this;
    }


    /**
     * Cloud service only.
     * <p>
     * Sets the table limits to use for the operation. Limits are used in only
     * 2 cases -- table creation statements and limits modification operations.
     * It is not used for other DDL operations.
     * <p>
     * If limits are set for an on-premise service they are silently ignored.
     *
     * @param tableLimits the limits
     *
     * @return this
     */
   public TableRequest setTableLimits(TableLimits tableLimits) {
        this.limits = tableLimits;
        return this;
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
    public TableRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /*
     * use table request timeout
     */
    @Override
    public TableRequest setDefaults(NoSQLHandleConfig config) {
        if (timeoutMs == 0) {
            timeoutMs = config.getDefaultTableRequestTimeout();
        }
        return this;
    }

    @Override
    public  void validate() {
        if (statement == null && tableName == null) {
            throw new IllegalArgumentException(
                "TableRequest requires statement or TableLimits and name");
        }
        if (statement != null && tableName != null) {
            throw new IllegalArgumentException(
                "Table Request cannot have both a table name and statement");
        }
        if (limits != null) {
            limits.validate();
        }
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createTableOpSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createTableOpDeserializer();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("TableRequest: [name=").append(tableName).append(",")
            .append("statement=").append(statement).append(",")
            .append("limits=").append(limits);
        return sb.toString();
    }

    /**
     * @hidden
     */
    @Override
    public boolean shouldRetry() {
        return false;
    }
}
