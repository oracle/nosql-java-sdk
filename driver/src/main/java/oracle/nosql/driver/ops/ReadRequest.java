/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;

/**
 * @hidden
 *
 * Represents a base class for read operations such as
 * {@link NoSQLHandle#get}.
 * <p>
 * This class encapsulates the common parameters of table name and
 * {@link Consistency}. By default read operations use
 * {@link Consistency#EVENTUAL}. Use of {@link Consistency#ABSOLUTE} should
 * be used only when required as it incurs additional cost.
 */
public abstract class ReadRequest extends Request {

    private Consistency consistency;

    protected ReadRequest() {}

    public Consistency getConsistencyInternal() {
        return consistency;
    }

    protected void setConsistencyInternal(Consistency consistency) {
        this.consistency = consistency;
    }

    /**
     * @hidden
     *
     * Return consistency if non-null. If null, return the default
     * Consistency from the config object
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
    public boolean doesReads() {
        return true;
    }
}
