/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;

/**
 * Represents the result of a {@link NoSQLHandle#multiDelete} operation.
 * <p>
 * On a successful operation the number of rows deleted is available using
 * {@link #getNumDeletions}. There is a limit to the amount of data consumed
 * by a single call. If there are still more rows to delete, the
 * continuation key can be get using {@link #getContinuationKey()}.
 * @see NoSQLHandle#multiDelete
 */
public class MultiDeleteResult extends Result {

    private byte[] continuationKey;
    private int nDeleted;

    /**
     * Returns the number of rows deleted from the table.
     *
     * @return the number of rows deleted
     */
    public int getNumDeletions() {
        return nDeleted;
    }

    /**
     * Returns the continuation key where the next MultiDelete request resume
     * from.
     *
     * @return the continuation key, or null if there are no more rows to
     * delete.
     */
    public byte[] getContinuationKey() {
        return continuationKey;
    }

    /* from Result */

    /**
     * Returns the read throughput consumed by this operation, in KBytes.
     * This is the actual amount of data read by the operation. The number
     * of read units consumed is returned by {@link #getReadUnits} which may
     * be a larger number because this was an update operation.
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
     * This number may be larger than that returned by {@link #getReadKB}
     * because it was an update operation.
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

    @Override
    public String toString() {
        return "Deleted " + nDeleted + " rows";
    }

    /**
     * @hidden
     * @param nDeleted num deleted
     * @return this
     */
    public MultiDeleteResult setNumDeletions(int nDeleted) {
        this.nDeleted = nDeleted;
        return this;
    }

    /**
     * @hidden
     * @param continuationKey the continuation key
     * @return this
     */
    public MultiDeleteResult setContinuationKey(byte[] continuationKey) {
        this.continuationKey = continuationKey;
        return this;
    }
}
