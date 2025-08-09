/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;

/**
 * Represents a base class for read operations such as
 * {@link NoSQLHandle#get}.
 */
public abstract class ReadRequest extends Request {

    private Consistency consistency;

    protected ReadRequest() {}

    /**
     * internal use only
     * @return the Consistency
     * @hidden
     */
    public Consistency getConsistencyInternal() {
        return consistency;
    }

    /**
     * @hidden
     */
    protected void setConsistencyInternal(Consistency consistency) {
        this.consistency = consistency;
    }

    /**
     *
     * Return consistency if non-null. If null, return the default
     * Consistency from the config object
     * @hidden
     */
    @Override
    public Request setDefaults(NoSQLHandleConfig config) {
        super.setDefaults(config);

        if (consistency == null) {
            consistency = config.getDefaultConsistency();
        }
        return this;
    }

    protected void validateReadRequest(String requestName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException(
                (requestName +
                 " requires table name"));
        }

        /*
         * If the request is within a transaction, the operation’s target table
         * must be within the hierarchy of the transaction's top level table.
         */
        if (transaction != null &&
            !transaction.getTableName()
                .equalsIgnoreCase(getTopTableName(tableName))) {
            throw new IllegalArgumentException(
                "The request table '" + tableName + "' is not in the " +
                "hierarchy of the transaction's target table '" +
                transaction.getTableName() + "'");
        }
    }

    /**
     * @hidden
     */
    @Override
    public boolean doesReads() {
        return true;
    }
}
