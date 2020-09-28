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
 * A request that encapsulates a query prepare call. Query preparation
 * allows queries to be compiled (prepared) and reused, saving time and
 * resources. Use of prepared queries vs direct execution of query strings
 * is highly recommended.
 * <p>
 * Prepared queries are implemented as {@link PreparedStatement}
 * which supports bind variables in queries which can be used to more easily
 * reuse a query by parameterization.
 * @see NoSQLHandle#prepare
 */
public class PrepareRequest extends Request {

    private String statement;

    private boolean getQueryPlan;

    public PrepareRequest() {}

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
    public PrepareRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Returns the query statement
     *
     * @return the statement, or null if it has not been set
     */
    public String getStatement() {
        return statement;
    }

    /**
     * Sets the query statement.
     *
     * @param statement the query statement
     *
     * @return this
     */
    public PrepareRequest setStatement(String statement) {
        this.statement = statement;
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
    public PrepareRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /**
     * Sets whether a printout of the query execution plan should be
     * included in the {@link PrepareResult}.
     *
     * @param v true if a printout of the query execution plan should be
     * included in the {@link PrepareResult}. False otherwise.
     *
     * @return this
     */
    public PrepareRequest setGetQueryPlan(boolean v) {
        getQueryPlan = v;
        return this;
    }

    /**
     * @return whether a prinout of the query execution plan should be include in
     * the {@link PrepareResult}.
     */
    public boolean getQueryPlan() {
        return getQueryPlan;
    }

    /**
     * Returns the timeout to use for the operation, in milliseconds. A value
     * of 0 indicates that the timeout has not been set.
     *
     * @return the value
     */
    public int getTimeout() {
        return super.getTimeoutInternal();
    }

    /**
     * Sets the table name for a query operation.
     * This is used by rate limiting logic to manage internal rate limiters.
     *
     * @param tableName the name (or OCID) of the table
     *
     * @return this
     */
    public PrepareRequest setTableName(String tableName) {
        super.setTableNameInternal(tableName);
        return this;
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createPrepareSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createPrepareDeserializer();
    }

    /**
     * @hidden
     */
    @Override
    public void validate() {
        if (statement == null){
            throw new IllegalArgumentException(
                "PrepareRequest requires a statement");
        }
    }
}
