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
    private boolean getQuerySchema;

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
    public PrepareRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /**
     * Sets the optional namespace.
     * On-premises only.
     *
     * This overrides any default value set with
     * {@link NoSQLHandleConfig#setDefaultNamespace}.
     * Note: if a namespace is specified in the table name in the SQL statement
     * (using the namespace:tablename format), that value will override this
     * setting.
     *
     * @param namespace the namespace to use for the operation
     *
     * @return this
     *
     * @since 5.4.10
     */
    public PrepareRequest setNamespace(String namespace) {
        super.setNamespaceInternal(namespace);
        return this;
    }

    /**
     * Sets whether the string value of the query execution plan should be
     * included in the {@link PrepareResult}.
     *
     * @param v true to include the query plan
     *
     * @return this
     */
    public PrepareRequest setGetQueryPlan(boolean v) {
        getQueryPlan = v;
        return this;
    }

    /**
     * @return whether the string value of the query execution plan should
     * be included in the {@link PrepareResult}.
     */
    public boolean getQueryPlan() {
        return getQueryPlan;
    }

    /**
     * Sets whether the JSON value of the query result schema
     * for the query should be included in the {@link PrepareResult}.
     *
     * @param v true to include the query schema
     *
     * @return this
     * @since 5.4
     */
    public PrepareRequest setGetQuerySchema(boolean v) {
        getQuerySchema = v;
        return this;
    }

    /**
     * @return whether the JSON value of the query result schema should
     * be included in the {@link PrepareResult}.
     * @since 5.4
     */
    public boolean getQuerySchema() {
        return getQuerySchema;
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

    @Override
    public String getTypeName() {
        return "Prepare";
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
