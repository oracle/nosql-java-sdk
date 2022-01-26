/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.values.MapValue;

/**
 * Represents the result of a {@link NoSQLHandle#delete} operation.
 * <p>
 * If the delete succeeded {@link #getSuccess} returns true.
 * Information about the existing row on failure may be
 * available using {@link #getExistingValue} and
 *.{@link #getExistingVersion}, depending on the use of
 * {@link DeleteRequest#setReturnRow}.
 * @see NoSQLHandle#delete
 */
public class DeleteResult extends WriteResult {
    private boolean success;

    /**
     * Returns true if the delete operation succeeded.
     *
     * @return true if the operation succeeded
     */
    public boolean getSuccess() {
        return success;
    }

    /**
     * Returns the existing row {@link Version} if available. It will be
     * available if the target row exists and the operation failed because of a
     * {@link Version} mismatch and the corresponding {@link DeleteRequest} the
     * method {@link DeleteRequest#setReturnRow} was called with a true value.
     *
     * @return the Version
     */
    public Version getExistingVersion() {
        return super.getExistingVersionInternal();
    }

    /**
     * Returns the existing row value if available. It will be available if the
     * target row exists and the operation failed because of a {@link Version}
     * mismatch and the corresponding {@link DeleteRequest} the method {@link
     * DeleteRequest#setReturnRow} was called with a true value.
     *
     * @return the value
     */
    public MapValue getExistingValue() {
        return super.getExistingValueInternal();
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
        return Boolean.toString(success);
    }

    /**
     * @hidden
     * @param success true if the operation succeeded
     * @return this
     */
    public DeleteResult setSuccess(boolean success) {
        this.success = success;
        return this;
    }
}
