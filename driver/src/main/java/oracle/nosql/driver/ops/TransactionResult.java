/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
package oracle.nosql.driver.ops;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandle;

/**
 * Represents the result of transaction operations including
 * {@link NoSQLHandle#beginTransaction}, {@link NoSQLHandle#commitTransaction}
 * and {@link NoSQLHandle#abortTransaction}.
 * <p>
 */
public class TransactionResult extends Result {

    private Transaction transaction;

    /**
     * Default constructor for TransactionResult
     */
    public TransactionResult() {}

    /**
     * Returns the transaction instance
     *
     * @return the transaction instance
     */
    public Transaction getTransaction() {
        return transaction;
    }

    /**
     * Sets the transaction instance
     *
     * @param transaction the transaction instance
     * @return this
     *
     * Internal use only
     * @hidden
     */
    public TransactionResult setTransaction(Transaction transaction) {
        this.transaction = transaction;
        return this;
    }

    /**
     * Returns the read throughput consumed by this operation, in KBytes.
     * This is the actual amount of data read by the operation. The number
     * of read units consumed is returned by {@link #getReadUnits} which may
     * be a larger number if the operation used {@link Consistency#ABSOLUTE}
     *
     * @return the read KBytes consumed
     */
    public int getReadKB() {
        return super.getReadKBInternal();
    }

    /**
     * Returns the write throughput consumed by this operation, in KBytes.
     *
     * @return the write KBytes consumed
     */
    public int getWriteKB() {
        return super.getWriteKBInternal();
    }

    /**
     * Returns the read throughput consumed by this operation, in read units.
     * This number may be larger than that returned by {@link #getReadKB} if
     * the operation used {@link Consistency#ABSOLUTE}
     *
     * @return the read units consumed
     */
    public int getReadUnits() {
        return super.getReadUnitsInternal();
    }

    /**
     * Returns the write throughput consumed by this operation, in write
     * units.
     *
     * @return the write units consumed
     */
    public int getWriteUnits() {
        return super.getWriteUnitsInternal();
    }
}
