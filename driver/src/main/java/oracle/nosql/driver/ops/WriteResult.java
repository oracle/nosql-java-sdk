/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.Version;
import oracle.nosql.driver.http.AsyncClient;
import oracle.nosql.driver.values.MapValue;

/**
 * A base class for results of single row modifying operations such as put
 * and delete.
 */
public class WriteResult extends Result {
    private Version existingVersion;
    private MapValue existingValue;
    private long existingModificationTime;
    private AsyncClient client;

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

    /**
     * @hidden
     * @return the modification time
     */
    public long getExistingModificationTimeInternal() {
        if (existingModificationTime < 0 && client != null) {
            client.oneTimeMessage("The requested feature is not supported by " +
                          "the connected server: getExistingModificationTime");
            return 0;
        }
        return existingModificationTime;
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

    /**
     * @hidden
     * @param existingModificationTime the modification time
     * @return this
     */
    public WriteResult setExistingModificationTime(
        long existingModificationTime) {
        this.existingModificationTime = existingModificationTime;
        return this;
    }

    /**
     * @hidden
     * for internal use
     * @param client the client
     */
    public void setClient(AsyncClient client) {
        this.client = client;
    }
}
