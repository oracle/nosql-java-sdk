/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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
    private long existingCreationTime;
    private long existingModificationTime;
    private String existingLastWriteMetadata;
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
     * Returns the associated write metadata
     * @return the write metadata or null
     * @since 5.4.18
     * @hidden
     */
    public String getExistingLastWriteMetadataInternal() {
        return existingLastWriteMetadata;
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
     * @return the creation time of the store row
     * @hidden
     */
    public long getExistingCreationTimeInternal() {
        if (existingCreationTime < 0 && client != null) {
            client.oneTimeMessage("The requested feature is not supported by " +
                          "the connected server: getExistingCreationTime");
            return 0;
        }
        return existingCreationTime;
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
     * @param creationTime the modification time
     * @return this
     * @hidden
     */
    public WriteResult setExistingCreationTime(
        long creationTime) {
        this.existingCreationTime = creationTime;
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
     * @param existingLastWriteMetadata the last write metadata
     * @return this
     * @since 5.4.18
     * @hidden
     */
    public WriteResult setExistingLastWriteMetadata(String existingLastWriteMetadata) {
        this.existingLastWriteMetadata = existingLastWriteMetadata;
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
