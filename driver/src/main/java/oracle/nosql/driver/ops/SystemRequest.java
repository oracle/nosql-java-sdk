/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
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
 * SystemRequest is an on-premise-only request used to perform any
 * table-independent administrative operation such as
 * create/drop of namespaces and security-relevant operations (create/drop
 * users and roles). These operations are asynchronous and completion needs
 * to be checked.
 * <p>
 * Examples of statements used in this object include:
 * <ul>
 * <li>CREATE NAMESPACE mynamespace</li>
 * <li>CREATE USER some_user IDENTIFIED BY password</li>
 * <li>CREATE ROLE some_role</li>
 * <li>GRANT ROLE some_role TO USER some_user</li>
 * </ul>
 * <p>
 * Execution of operations specified by this request is implicitly asynchronous.
 * These are potentially long-running operations.
 * {@link NoSQLHandle#systemRequest} returns a {@link SystemResult} instance that
 * can be used to poll until the operation succeeds or fails.
 * <p>
 * The statements are passed as char[] because some statements will include
 * passwords and using an array allows the memory to be cleared to avoid
 * keeping sensitive information in memory.
 *
 * @see NoSQLHandle#systemRequest
 */
public class SystemRequest extends Request {
    private char[] statement;

    /**
     * Returns the statement, or null if not set
     *
     * @return the statement
     */
    public char[] getStatement() {
        return statement;
    }

    /**
     * Sets the statement to use for the operation. This parameter is
     * required.
     *
     * @param statement the statement
     *
     * @return this
     */
    public SystemRequest setStatement(char[] statement) {
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
    public SystemRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /*
     * use the default request timeout if not set.
     */
    @Override
    public SystemRequest setDefaults(NoSQLHandleConfig config) {
        if (timeoutMs == 0) {
            timeoutMs = config.getDefaultTableRequestTimeout();
        }
        return this;
    }

    @Override
    public  void validate() {
        if (statement == null) {
            throw new IllegalArgumentException(
                "SystemRequest requires a statement");
        }
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createSystemOpSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createSystemOpDeserializer();
    }

    @Override
    public String getTypeName() {
        return "System";
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("SystemRequest: [statement=").append(new String(statement))
            .append("]");
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
