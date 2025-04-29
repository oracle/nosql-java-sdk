/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.JsonParseException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.JsonUtils;

/**
 * Represents a base class for the single row modifying operations
 * {@link NoSQLHandle#put} and {@link NoSQLHandle#delete}.
 */
public abstract class WriteRequest extends DurableRequest {

    private boolean returnRow;
    private String rowMetadata;

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
     * Returns the row metadata to be used for this request.
     *
     * @return the metadata, or null if not set
     *
     * @since 5.4.18
     */
    public String getRowMetadata() {
        return rowMetadata;
    }

    /**
     * Sets the row metadata to use for this request.
     * This is an optional parameter.
     * The @parameter rowMetadata must be in a JSON Object format or null,
     * otherwise an IllegalArgumentException is thrown.
     *
     * @param rowMetadata the row metadata
     * @throws IllegalArgumentException if rowMetadata not null and invalid
     * JSON Object format
     *
     * @since 5.4.18
     * @return this
     */
    public WriteRequest setRowMetadata(String rowMetadata) {
        if (rowMetadata == null) {
            this.rowMetadata = null;
            return this;
        }

        JsonUtils.validateJsonObject(rowMetadata);
        this.rowMetadata = rowMetadata;
        return this;
    }
}
