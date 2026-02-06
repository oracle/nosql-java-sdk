/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
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
 * Represents the argument of a {@link NoSQLHandle#getTable} operation which
 * returns static information associated with a table, as returned in
 * {@link TableResult}. This information only changes in response to a change
 * in table schema or a change in provisioned throughput or capacity for the
 * table.
 * @see NoSQLHandle#getTable
 */
public class GetTableRequest extends Request {
    private String operationId;

    /**
     * Sets the table name to use for the request
     *
     * @param tableName the table name. This is a required parameter.
     *
     * @return this
     */
    public GetTableRequest setTableName(String tableName) {
        super.setTableNameInternal(tableName);
        return this;
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
    public GetTableRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Sets the operation id to use for the request. The operation id can be
     * obtained via {@link TableResult#getOperationId}. This parameter is
     * optional. If non-null, it represents an asynchronous table operation that
     * may be in progress. It is used to examine the result of the operation and
     * if the operation has failed an exception will be thrown in response to
     * a {@link NoSQLHandle#getTable} operation. If the operation is in progress
     * or has completed successfully, the state of the table is returned.
     *
     * @param operationId the operationId. This is optional.
     *
     * @return this
     */
    public GetTableRequest setOperationId(String operationId) {
        this.operationId = operationId;
        return this;
    }

    /**
     * Returns the operation id to use for the request, null if not set.
     *
     * @return the operation id
     */
    public String getOperationId() {
        return operationId;
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
    public GetTableRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /**
     * Sets the optional namespace.
     * On-premises only.
     *
     * This overrides any default value set with
     * {@link NoSQLHandleConfig#setDefaultNamespace}.
     * Note: if a namespace is specified in the table name for the request
     * (using the namespace:tablename format), that value will override this
     * setting.
     *
     * @param namespace the namespace to use for the operation
     *
     * @return this
     *
     * @since 5.4.10
     */
    public GetTableRequest setNamespace(String namespace) {
        super.setNamespaceInternal(namespace);
        return this;
    }

    /**
     * @hidden
     */
    @Override
    public void validate() {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException(
                "GetTableRequest requires a table name");
        }
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createGetTableSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createGetTableDeserializer();
    }

    @Override
    public String getTypeName() {
        return "GetTable";
    }

    /**
     * @hidden
     */
    @Override
    public boolean shouldRetry() {
        return false;
    }
}
