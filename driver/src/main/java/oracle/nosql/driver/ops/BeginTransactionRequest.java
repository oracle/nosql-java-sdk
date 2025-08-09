/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.Durability;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;

/**
 * Represents the input to a {@link NoSQLHandle#beginTransaction} operation
 * which is used to begin a Transaction.
 *
 * @since 5.4.x
 */
public class BeginTransactionRequest extends DurableRequest {

    /**
     * TransactionIsolation defines the transaction isolation level, which
     * controls the visibility and interaction of data changes among concurrent
     * transactions.
     */
    public enum TransactionIsolation {
        /**
         * All rows that a transaction retrieves are committed writes, in
         * other words, dirty reads (aka uncommitted read) are not permitted
         * in transaction. This is the default transaction isolation level.
         */
        READ_COMMITTED,

        /**
         * A dirty read (aka uncommitted read) occurs when a transaction
         * retrieves a row that has been updated by another transaction that
         * is not yet committed.
         */
        READ_UNCOMMITTED
    }

    /**
     * Transaction isolation level
     */
    private TransactionIsolation isolation;

    /**
     * The maximum number of writes operations in the transaction
     */
    private long maxNumWrites;

    /**
     * The transaction timeout in milliseconds
     */
    private long txnTimeoutMs;

    /**
     * Default constructor for BeginTransactionRequest
     */
    public BeginTransactionRequest() {
    }

    /**
     * Sets the name of the top-level table in the hierarchy that defines the
     * scope of this transaction. This is a required parameter.
     *
     * @param tableName the name of the top-level table in the hierarchy
     * @return this {@link BeginTransactionRequest} instance
     */
    public BeginTransactionRequest setTableName(String tableName) {
        setTableNameInternal(getTopTableName(tableName));
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
    public BeginTransactionRequest setCompartmentId(String compartment) {
        setCompartmentInternal(compartment);
        return this;
    }

    /**
     * On-premises only.
     *
     * Sets the durability for the transaction, this durability applies to all
     * the operations in the transaction.
     *
     * @param durability the durability value. Set to null for
     * the default durability setting on the server.
     * @return this {@link BeginTransactionRequest} instance
     */
    public BeginTransactionRequest setDurablity(Durability durability) {
        setDurabilityInternal(durability);
        return this;
    }

    /**
     * Returns the isolation setting of this transaction.
     *
     * @return isolation, if set, otherwise null.
     */
    public TransactionIsolation getIsolation() {
        return isolation;
    }

    /**
     * Sets the isolation to use for the transaction.
     *
     * @param isolation the isolation value. Set to null for
     * the default isolation setting on the server.
     *
     * @return this {@link BeginTransactionRequest} instance
     */
    public BeginTransactionRequest setIsolation(TransactionIsolation isolation) {
        this.isolation = isolation;
        return this;
    }

    /**
     * Returns the maximum number of writes allowed in the transaction
     *
     * @return the maximum number of writes, or 0 if not set
     */
    public long getMaxNumWrites() {
        return maxNumWrites;
    }

    /**
     * Sets the maximum number of writes allowed in the transaction.
     *
     * @param maxNumWrites the number of writes
     *
     * @return this {@link BeginTransactionRequest} instance
     *
     * @throws IllegalArgumentException if the value is less than or equal to 0
     */
    public BeginTransactionRequest setMaxNumWrites(long maxNumWrites) {
        if (maxNumWrites < 0) {
            throw new IllegalArgumentException("maxNumWrites must be >= 0");
        }
        this.maxNumWrites = maxNumWrites;
        return this;
    }

    /**
     * Returns the transaction timeout in milliseconds
     *
     * @return the transaction timeout, or 0 if not set
     */
    public long getTransactionTimeout() {
        return txnTimeoutMs;
    }

    /**
     * Sets the transaction timeout value, in milliseconds.
     *
     * @param txnTimeoutMs the timeout value, in milliseconds
     *
     * @return this {@link BeginTransactionRequest} instance
     *
     * @throws IllegalArgumentException if the timeout value is less than
     * or equal to 0
     */
    public BeginTransactionRequest setTransactionTimeout(long txnTimeoutMs) {
        if (txnTimeoutMs < 0) {
            throw new IllegalArgumentException(
                "TransactionTimeout must be >= 0");
        }
        this.txnTimeoutMs = txnTimeoutMs;
        return this;
    }

    /**
     * Sets the request timeout value, in milliseconds. This overrides any
     * default value set with {@link NoSQLHandleConfig#setRequestTimeout}.
     * The value must be positive.
     *
     * @param timeoutMs the timeout value, in milliseconds
     *
     * @return this {@link BeginTransactionRequest} instance
     *
     * @throws IllegalArgumentException if the timeout value is less than
     * or equal to 0
     */
    public BeginTransactionRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /**
     * @hidden
     */
    @Override
    public void validate() {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException(
                "BeginTransactionRequest requires table name");
        }
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createBeginTransactionSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createBeginTransactionDeserializer();
    }

    @Override
    public String getTypeName() {
        return "BeginTransaction";
    }
 }