/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.util.concurrent.atomic.AtomicBoolean;

/**
 * Represents a transaction that can be used to perform multiple operations
 * on a top-level table and its hierarchy of child tables, with all operations
 * sharing the same shard key.
 *<p>
 * A transaction is active once it has started and remains active until it
 * is either committed or aborted. Attempting to use an inactive transaction
 * in an operation results in an {@link IllegalArgumentException}.
 *<p>
 * Transaction object is thread-safe and can be used for concurrent operations.
 *<p>
 * The statistical information of the transaction such as {@code numWrites},
 * {@code numReads}, and {@code elapsedTimeMs} becomes available after the
 * transaction is committed or aborted.
 */
public class Transaction {
    /*
     * The compartment of the table hierarchy associated with this transaction.
     */
    private String compartment;

    /*
     * The name of the top-level table in the hierarchy associated with this
     * transaction. The transaction can operate only within this table and its
     * hierarchy of child tables.
     */
    private String tableName;

    /*
     * Indicates whether the transaction is active, it is true once the
     * transaction begins, and becomes false after the transaction has been
     * committed or aborted.
     */
    private boolean isActive;

    /*
     * The serialized representation of the KV transaction.
     */
    private byte[] kvTxnBytes;

    /*
     * Indicates whether an operation has been bound to the transaction, the
     * binding operation determines the transaction's shard key, and only one
     * operation can be bound to a transaction
     */
    private final AtomicBoolean boundToOperation;

    /*
     * Indicates whether the binding operation has completed successfully.
     *
     * All other operations using the same transaction will be blocked until the
     * binding operation is finished.
     *
     * If the binding operation fails, it is unbound from the transaction,
     * and the next eligible operation will become the binding operation.
     */
    private boolean bindingOpDone;

    /*
     * The following statistic information are available after the transaction
     * is committed or aborted.
     */

    /*
     * The number of writes performed in the transaction
     */
    private long numWrites;
    /*
     * The number of reads performed in the transaction
     */
    private long numReads;
    /*
     * The duration of the transaction in milliseconds.
     */
    private long elapsedTimeMs;

    public Transaction(String compartment, String tableName) {
        this.compartment = compartment;
        this.tableName = tableName;
        this.kvTxnBytes = null;
        boundToOperation = new AtomicBoolean(false);
        bindingOpDone = false;
        isActive = true;
    }

    /**
     * Returns the compartment id or name.
     *
     * Cloud service only.
     *
     * @return compartment id or name if set for the request
     */
    public String getCompartment() {
        return compartment;
    }

    /**
     * Returns the top-level table name in the hierarchy for this transaction.
     * The transaction is limited to this table and all its child tables.
     *
     * @return the top-level table name
     */
    public String getTableName() {
        return tableName;
    }

    /**
     * Returns the number of writes performed in the transaction, the value is
     * available after the transaction is committed or aborted.
     *
     * @return the number of writes
     */
    public long getNumWrites() {
        return numWrites;
    }

    /**
     * Returns the number of reads performed in the transaction, the value is
     * available after the transaction is committed or aborted.
     *
     * @return the number of reads
     */
    public long getNumReads() {
        return numReads;
    }

    /**
     *
     * Returns the elapsed time in milliseconds of the transaction, the value is
     * available after the transaction is committed or aborted.
     *
     * @return the elapsed time in milliseconds
     */
    public long getElapsedTimeMs() {
        return elapsedTimeMs;
    }

    /**
     * Returns {@code true} if the transaction has been started and is not yet
     * committed or aborted
     *
     * @return {@code true} if the transaction is in progress, otherwise
     * return {@code false}
     */
    public boolean isActive() {
        return isActive;
    }

    /**
     * Internal use only
     *
     * Returns the serialized transaction. The byte array returned is opaque
     * to applications and is interpreted by the server.
     *
     * @return the serialized transaction
     * @hidden
     */
    public byte[] getKVTransaction() {
        return kvTxnBytes;
    }

    /**
     * Internal use only
     * @Hidden
     */
    public Transaction setKVTransaction(byte[] kvTxnBytes) {
        this.kvTxnBytes = kvTxnBytes;
        return this;
    }

    /**
     * Attempts to bind an operation to this transaction if it hasn't been bound
     * yet.
     *
     * This method uses a compare-and-set operation to atomically check if the
     * transaction is currently unbound and, if so, binds it to an operation.
     * If the transaction is already bound, this method does nothing and returns
     * false.
     *
     * @return true if the operation was successfully bound, false otherwise
     * @hidden Internal use only
     */
    public boolean bindOperationIfUnbound() {
        return boundToOperation.compareAndSet(false, true);
    }

    /**
     * Internal use only
     * @Hidden
     */
    public boolean isOperationBound() {
        return boundToOperation.get();
    }

    /**
     * Internal use only
     *
     * Attempts to unbind an operation from this transaction.
     *
     * This method unbinds the current operation from the transaction to ensure
     * that subsequent operation can be bound with the transaction. It is used
     * when binding operation fails.
     *
     * @return true if the operation was successfully unbound, false otherwise
     * @hidden
     */
    public boolean unbindOperationIfBound() {
        return boundToOperation.compareAndSet(true, false);
    }

    /**
     * Internal use only
     * @Hidden
     */
    public void setBindingOpDone(boolean value) {
        bindingOpDone = value;
    }

    /**
     * Internal use only
     * @Hidden
     */
    public boolean isBindingOpDone() {
        return bindingOpDone;
    }

    /**
     * Internal use only
     *
     * Sets the number of writes performed in the transaction
     *
     * @param numWrites the number of writes performed in the transaction
     * @hidden
     */
    public void setNumWrites(long numWrites) {
        this.numWrites = numWrites;
    }

    /**
     * Internal use only
     *
     * Sets the number of reads performed in the transaction
     *
     * @param numReads the number of reads performed in the transaction
     * @hidden
     */
    public void setNumReads(long numReads) {
        this.numReads = numReads;
    }

    /**
     * Internal use only
     *
     * Sets the Transaction elapsed time in milliseconds
     *
     * @param timeMs the Transaction elapsed time in milliseconds
     * @hidden
     */
    public void setElapsedTimeMs(long timeMs) {
        elapsedTimeMs = timeMs;
    }

    /**
     * Internal use only
     * @hidden
     */
    public void setIsActive(boolean isActive) {
        this.isActive = isActive;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder()
            .append("Transaction: [compartment=").append(compartment)
            .append(", tableName=").append(tableName)
            .append(", boundToOperation=").append(boundToOperation)
            .append(", bindingOpDone=").append(bindingOpDone)
            .append(", numWrites=").append(numWrites)
            .append(", numReads=").append(numReads)
            .append(", elapsedTimeMs=").append(elapsedTimeMs)
            .append("]");
        return sb.toString();
    }
}