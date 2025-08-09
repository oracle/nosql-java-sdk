/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;

/**
 * Represents the input to a {@link NoSQLHandle#abortTransaction} operation
 * which is used to abort a transaction.
 *
 * @see NoSQLHandle#abortTransaction(AbortTransactionRequest)
 * @see Transaction
 *
 * @since 5.4.x
 */
public class AbortTransactionRequest extends EndTransactionRequest {

    public AbortTransactionRequest() {
        super(Type.ABORT);
    }

    /**
     * Sets the transaction to be aborted.
     *
     * @param txn the {@link Transaction} instance
     * @return this {@link AbortTransactionRequest} instance
     */
    public AbortTransactionRequest setTransaction(Transaction txn) {
        super.setTransactionInternal(txn);
        return this;
    }

    /**
     * Returns the transaction instance to be aborted by this operation.
     *
     * @return the transaction instance, or null if not set
     */
    public Transaction getTransaction() {
        return super.getTransactionInternal();
    }

    @Override
    public String getTypeName() {
        return "AbortTransactionRequest";
    }
}