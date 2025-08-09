/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;

/**
 * Internal use only
 * @hidden
 *
 * Base class for requests that terminate a transaction, such as
 * {@link CommitTransactionRequest} or {@link AbortTransactionRequest}.
 */
public abstract class EndTransactionRequest extends Request {

    public enum Type {
        COMMIT,
        ABORT
    };

    private final Type type;

    EndTransactionRequest(Type type) {
        this.type = type;
    }

    /**
     * Sets the transaction to be ended.
     * <p>
     * This method sets the compartment, table name, and transaction instance
     * based on the provided {@link Transaction} object.
     *
     * @param txn the {@link Transaction} instance to be ended
     *
     * Internal use only
     * @hidden
     */
    @Override
    public void setTransactionInternal(Transaction txn) {
        setCompartmentInternal(txn.getCompartment());
        setTableNameInternal(txn.getTableName());
        super.setTransactionInternal(txn);
    }

    /**
     * Returns the type of the transaction to be ended.
     * <p>
     * The type can be either {@link Type#COMMIT} to commit the transaction or
     * {@link Type#ABORT} to abort the transaction.
     *
     * @return the type of the transaction to be ended
     */
    public Type getType() {
        return type;
    }

    /**
     * Validates the state of the members of this class for use.
     *
     * @throws IllegalArgumentException if state is invalid
     *
     * @hidden
     * Internal use only
     */
    @Override
    public void validate() {
        if (getTransactionInternal() == null) {
            throw new IllegalArgumentException(
                getTypeName() + " requires a transaction");
        }
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createEndTransactionSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createEndTransactionDeserializer();
    }
}