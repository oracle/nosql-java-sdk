/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;

/**
 * On-premises only.
 * <p>
 * SystemStatusRequest is an on-premise-only request used to check the status
 * of an operation started using a {@link SystemRequest}.
 *
 * @see NoSQLHandle#systemRequest
 * @see NoSQLHandle#systemStatus
 */
public class SystemStatusRequest extends Request {
    private String statement;
    private String operationId;

    /**
     * Returns the statement, or null if not set
     *
     * @return the statement
     */
    public String getStatement() {
        return statement;
    }

    /**
     * Sets the statement that was used for the operation. This is optional
     * and is not used in any significant way. It is returned, unmodified,
     * in the {@link SystemResult} for convenience.
     *
     * @param statement the statement
     *
     * @return this
     */
    public SystemStatusRequest setStatement(String statement) {
        this.statement = statement;
        return this;
    }

    /**
     * Sets the operation id to use for the request. The operation id can be
     * obtained via {@link SystemResult#getOperationId}. This parameter is not
     * optional and represents an asynchronous operation that
     * may be in progress. It is used to examine the result of the operation and
     * if the operation has failed an exception will be thrown in response to
     * a {@link NoSQLHandle#systemStatus} operation. If the operation is in
     * progress or has completed successfully, the state of the operation is
     * returned.
     *
     * @param operationId the operationId.
     *
     * @return this
     */
    public SystemStatusRequest setOperationId(String operationId) {
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
    public SystemStatusRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /*
     * use the default request timeout if not set.
     */
    @Override
    public SystemStatusRequest setDefaults(NoSQLHandleConfig config) {
        if (timeoutMs == 0) {
            timeoutMs = config.getDefaultTableRequestTimeout();
        }
        return this;
    }

    @Override
    public  void validate() {
        if (operationId == null) {
            throw new IllegalArgumentException(
                "SystemStatusRequest requires an operation id");
        }
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createSystemStatusSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createSystemStatusDeserializer();
    }

    @Override
    public String getTypeName() {
        return "SystemStatus";
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SystemStatusRequest [statement= ").append(statement)
            .append(", operationId = ").append(operationId).append("]");
        return sb.toString();
    }

    /**
     * @hidden
     */
    @Override
    public boolean shouldRetry() {
        return true;
    }
}
