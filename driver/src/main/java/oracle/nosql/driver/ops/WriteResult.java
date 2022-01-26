/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.Version;
import oracle.nosql.driver.values.MapValue;

/**
 * @hidden
 *
 * A base class for results of single row modifying operations such as put
 * and delete.
 */
public class WriteResult extends Result {
    private Version existingVersion;
    private MapValue existingValue;

    protected WriteResult() {}

    /**
     * @hidden
     * getters are internal for delegation from sub-classes
     * @return the Version or null
     */
    public Version getExistingVersionInternal() {
        return existingVersion;
    }

    /**
     * @hidden
     * @return the value or null
     */
    public MapValue getExistingValueInternal() {
        return existingValue;
    }

    /*
     * Setters are internal use only
     */

    /**
     * @hidden
     * @param existingVersion the version
     * @return this
     */
    public WriteResult setExistingVersion(Version existingVersion) {
        this.existingVersion = existingVersion;
        return this;
    }

    /**
     * @hidden
     * @param existingValue the value
     * @return this
     */
    public WriteResult setExistingValue(MapValue existingValue) {
        this.existingValue = existingValue;
        return this;
    }
}
