/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.values.JsonUtils;

/**
 * Represents a base class for the single row modifying operations
 * {@link NoSQLHandle#put} and {@link NoSQLHandle#delete}.
 */
public abstract class WriteRequest extends DurableRequest {

    private boolean returnRow;
    private String lastWriteMetadata;

    protected WriteRequest() {}

    protected void setReturnRowInternal(boolean value) {
        this.returnRow = value;
    }

    /* getters are public for access by serializers */

    /**
     * internal use only
     * @return true if there is a return row
     * @hidden
     */
    public boolean getReturnRowInternal() {
        return returnRow;
    }

    /**
     * @hidden
     */
    @Override
    public Request setDefaults(NoSQLHandleConfig config) {
        super.setDefaults(config);
        return this;
    }

    protected void validateWriteRequest(String requestName) {
        if (tableName == null || tableName.isEmpty()) {
            throw new IllegalArgumentException(
                (requestName +
                 " requires table name"));
        }
    }

    /**
     * Returns the last write metadata to be used for this request.
     *
     * @return the last write metadata, or null if not set
     *
     * @since 5.4.20
     */
    public String getLastWriteMetadata() {
        return lastWriteMetadata;
    }

    /**
     * Sets the write metadata to use for this request.
     * This is an optional parameter.<p>
     *
     * Last write metadata is associated to a certain version of a row. Any
     * subsequent write operation will use its own write metadata value. If not
     * specified null will be used by default.
     * NOTE that if you have previously written a record with metadata and a
     * subsequent write does not supply metadata, the metadata associated with
     * the row will be null. Therefore, if you wish to have metadata
     * associated with every write operation, you must supply a valid JSON
     * construct to this method.<p>
     *
     * @param lastWriteMetadata the write metadata, must be null or a valid JSON
     *    construct: object, array, string, number, true, false or null,
     *    otherwise an IllegalArgumentException is thrown.
     * @throws IllegalArgumentException if lastWriteMetadata not null and invalid
     *    JSON construct
     *
     * @since 5.4.20
     * @return this
     */
    public WriteRequest setLastWriteMetadata(String lastWriteMetadata) {
        if (lastWriteMetadata == null) {
            this.lastWriteMetadata = null;
            return this;
        }

        JsonUtils.validateJsonConstruct(lastWriteMetadata);
        this.lastWriteMetadata = lastWriteMetadata;
        return this;
    }
}
