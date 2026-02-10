/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.http.Client;
import oracle.nosql.driver.values.MapValue;

/**
 * Represents the result of a {@link NoSQLHandle#get} operation.
 * <p>
 * On a successful operation the value of the row is available using
 * {@link #getValue} and the other state available in this class is valid.
 * On failure that value is null and other state, other than consumed
 * capacity, is undefined.
 * @see NoSQLHandle#get
 */
public class GetResult extends Result {

    private MapValue value;
    private Version version;
    private long expirationTime;
    private long creationTime;
    private long modificationTime;
    private Client client;
    private String lastWriteMetadata;

    /**
     * Returns the value of the returned row, or null if the row does not exist
     *
     * @return the value of the row, or null if it does not exist
     */
    public MapValue getValue() {
        return value;
    }

    /**
     * Returns a JSON string representation of the returned row, or null if the
     * row does not exist.
     *
     * @return the JSON string value of the row, or null if it does not exist
     */
    public String getJsonValue() {
        return (value != null) ? value.toJson() : null;
    }

    /**
     * Returns the {@link Version} of the row if the operation was successful,
     * or null if the row does not exist.
     *
     * @return the version of the row, or null if the row does not exist
     */
    public Version getVersion() {
        return version;
    }

    /**
     * Returns the expiration time of the row. A zero value indicates that the
     * row does not expire. This value is valid only if the operation
     * successfully returned a row ({@link #getValue} returns non-null).
     *
     * @return the expiration time in milliseconds since January 1, 1970, GMT,
     * or zero if the row never expires or the row does not exist
     */
    public long getExpirationTime() {
        return expirationTime;
    }

    /**
     * Returns the creation time of the row.
     * This value is valid only if the operation
     * successfully returned a row ({@link #getValue} returns non-null).
     *
     * Note: If the row was written by a version of the system older than 25.3
     * the creation time will be equal to the modification time, if it was
     * written by a system older than 19.5 it will be zero.
     *
     * @return the creation time in milliseconds since January 1, 1970, GMT,
     * or zero if the row does not exist
     *
     * @since 5.4.18
     * @hidden
     */
    public long getCreationTime() {
        if (creationTime < 0 && client != null) {
            client.oneTimeMessage("The requested feature is not supported by " +
                "the connected server: getCreationTime");
            return 0;
        }
        return creationTime;
    }

    /**
     * Returns the modification time of the row.
     * This value is valid only if the operation
     * successfully returned a row ({@link #getValue} returns non-null).
     *
     * @return the modification time in milliseconds since January 1, 1970, GMT,
     * or zero if the row does not exist
     *
     * @since 5.3.0
     */
    public long getModificationTime() {
        if (modificationTime < 0 && client != null) {
            client.oneTimeMessage("The requested feature is not supported by " +
                "the connected server: getModificationTime");
            return 0;
        }
        return modificationTime;
    }

    /**
     * Internal use only.
     *
     * Sets the value of this object
     *
     * @param value the value of the row
     *
     * @return this
     * @hidden
     */
    public GetResult setValue(MapValue value) {
        this.value = value;
        return this;
    }

    /**
     * Returns the metadata used on last write of the returned row, or null if
     * the row does not exist or metadata was not set.
     *
     * @return the metadata used on last write of the row, or null if row does
     * not exist or not set
     *
     * @since 5.4.20
     */
    public String getLastWriteMetadata() {
        return lastWriteMetadata;
    }

    /**
     * Internal use only.<p>
     *
     * Sets the lastWriteMetadata of this object.
     *
     * @param lastWriteMetadata the write metadata
     *
     * @return this
     *
     * @since 5.4.20
     * @hidden
     */
    public GetResult setLastWriteMetadata(String lastWriteMetadata) {
        this.lastWriteMetadata = lastWriteMetadata;
        return this;
    }

    /**
     * Internal use only.
     *
     * Sets the expiration time.
     *
     * @param expirationTime the expiration time
     *
     * @return this
     * @hidden
     */
    public GetResult setExpirationTime(long expirationTime) {
        this.expirationTime = expirationTime;
        return this;
    }

    /**
     * Internal use only.
     *
     * Sets the creation time.
     *
     * @param creationTime the creation time
     *
     * @return this
     * @hidden
     */
    public GetResult setCreationTime(long creationTime) {
        this.creationTime = creationTime;
        return this;
    }

    /**
     * Internal use only.
     *
     * Sets the modification time.
     *
     * @param modificationTime the modification time
     *
     * @return this
     * @hidden
     */
    public GetResult setModificationTime(long modificationTime) {
        this.modificationTime = modificationTime;
        return this;
    }

    /**
     * Internal use only.
     *
     * Sets the version.
     *
     * @param version the version
     *
     * @return this
     * @hidden
     */
    public GetResult setVersion(Version version) {
        this.version = version;
        return this;
    }

    /* from Result */

    /**
     * Returns the read throughput consumed by this operation, in KBytes.
     * This is the actual amount of data read by the operation. The number
     * of read units consumed is returned by {@link #getReadUnits} which may
     * be a larger number if the operation used {@link Consistency#ABSOLUTE}
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
     * This number may be larger than that returned by {@link #getReadKB} if
     * the operation used {@link Consistency#ABSOLUTE}
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
        return getJsonValue();
    }

    /**
     * for internal use
     * @param client the Client
     * @hidden
     */
    public void setClient(Client client) {
        this.client = client;
    }
}
