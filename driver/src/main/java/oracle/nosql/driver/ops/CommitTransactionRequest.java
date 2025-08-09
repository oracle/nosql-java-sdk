/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;

/**
 * Represents the input to a {@link NoSQLHandle#commitTransaction} operation
 * which is used to commit a transaction.
 *
 * @see NoSQLHandle#commitTransaction(CommitTransactionRequest)
 * @see Transaction
 *
 * @since 5.4.x
 */
public class CommitTransactionRequest extends EndTransactionRequest {
    /**
     * Creates a new instance of {@link CommitTransactionRequest}.
     * <p>
     * This constructor initializes the request with a type of {@link Type#COMMIT},
     * indicating that the transaction is to be committed.
     */
    public CommitTransactionRequest() {
        super(Type.COMMIT);
    }

    /**
     * Sets the transaction to be committed.
     *
     * @param txn the {@link Transaction} instance
     * @return this {@link CommitTransactionRequest} instance
     */
    public CommitTransactionRequest setTransaction(Transaction txn) {
        super.setTransactionInternal(txn);
        return this;
    }

    /**
     * Returns the transaction instance to be committed by this operation.
     *
     * @return the transaction instance, or null if not set
     */
    public Transaction getTransaction() {
        return super.getTransactionInternal();
    }

    @Override
    public String getTypeName() {
        return "CommitTransactionRequest";
    }
}