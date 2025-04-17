/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;

/**
 * Represents a base class for the single row modifying operations
 * {@link NoSQLHandle#put} and {@link NoSQLHandle#delete}.
 */
public abstract class WriteRequest extends DurableRequest {

    private boolean returnRow;

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
}
