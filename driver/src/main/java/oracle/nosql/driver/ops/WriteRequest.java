/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;

/**
 * @hidden
 *
 * Represents a base class for the single row modifying operations
 * {@link NoSQLHandle#put} and {@link NoSQLHandle#delete}.
 * <p>
 * This class encapsulates the common parameters of table name and
 * the return row boolean, which allows applications to get information
 * about the existing value of the target row on failure. By default
 * no previous information is returned.
 */
public abstract class WriteRequest extends Request {

    private boolean returnRow;

    protected WriteRequest() {}

    protected void setReturnRowInternal(boolean value) {
        this.returnRow = value;
    }

    /* getters are public for access by serializers */

    /**
     * @hidden
     * @return true if there is a return row
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
        if (tableName == null) {
            throw new IllegalArgumentException(
                (requestName +
                 " requires table name"));
        }
    }

    /**
     * @hidden
     */
    @Override
    public boolean doesWrites() {
        return true;
    }
}
