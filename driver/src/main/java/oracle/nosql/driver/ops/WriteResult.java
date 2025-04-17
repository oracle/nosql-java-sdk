/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.Version;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.http.Client;

/**
 * A base class for results of single row modifying operations such as put
 * and delete.
 */
public class WriteResult extends Result {
    private Version existingVersion;
    private MapValue existingValue;
    private long existingModificationTime;
    private String existingRowMetadata;
    private Client client;

    protected WriteResult() {}

    /**
     * getters are internal for delegation from sub-classes
     * @return the Version or null
     * @hidden
     */
    public Version getExistingVersionInternal() {
        return existingVersion;
    }

    /**
     * Returns the associated row metadata
     * @return the row metadata or null
     * @hidden
     */
    public String getExistingRowMetadataInternal() {
        return existingRowMetadata;
    }

    /**
     * internal use only
     * @return the value or null
     * @hidden
     */
    public MapValue getExistingValueInternal() {
        return existingValue;
    }

    /**
     * internal use only
     * @return the modification time
     * @hidden
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
     * internal use only
     * @param existingVersion the version
     * @return this
     * @hidden
     */
    public WriteResult setExistingVersion(Version existingVersion) {
        this.existingVersion = existingVersion;
        return this;
    }

    /**
     * internal use only
     * @param existingValue the value
     * @return this
     * @hidden
     */
    public WriteResult setExistingValue(MapValue existingValue) {
        this.existingValue = existingValue;
        return this;
    }

    /**
     * internal use only
     * @param existingModificationTime the modification time
     * @return this
     * @hidden
     */
    public WriteResult setExistingModificationTime(
        long existingModificationTime) {
        this.existingModificationTime = existingModificationTime;
        return this;
    }

    /**
     * Internal use only.
     *
     * @param existingRowMetadata the row metadata
     * @return this
     * @hidden
     */
    public WriteResult setExistingRowMetadata(String existingRowMetadata) {
        this.existingRowMetadata = existingRowMetadata;
        return this;
    }

    /**
     * for internal use
     * @param client the client
     * @hidden
     */
    public void setClient(Client client) {
        this.client = client;
    }
}
