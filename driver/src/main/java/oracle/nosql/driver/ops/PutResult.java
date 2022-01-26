/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;

/**
 * Represents the result of a {@link NoSQLHandle#put} operation.
 * <p>
 * On a successful operation the value returned by {@link #getVersion} is
 * non-null. On failure that value is null. Information about the
 * existing row on failure may be available using
 * {@link #getExistingValue} and.{@link #getExistingVersion}, depending on the
 * use of {@link PutRequest#setReturnRow} and whether the put had an option set
 * using {@link PutRequest#setOption}.
 * @see NoSQLHandle#put
 */
public class PutResult extends WriteResult {
    private Version version;
    private FieldValue generatedValue;

    /**
     * Returns the {@link Version} of the new row if the operation was
     * successful. If the operation failed null is returned.
     *
     * @return the {@link Version} on success, null on failure
     */
    public Version getVersion() {
        return version;
    }

    /**
     * @hidden
     * @param version the version
     * @return this
     */
    public PutResult setVersion(Version version) {
        this.version = version;
        return this;
    }

    /**
     * Returns the existing row {@link Version} if available. This value will
     * only be available if the conditional put operation failed and the request
     * specified that return information be returned using
     * {@link PutRequest#setReturnRow}.
     *
     * @return the Version
     */
    public Version getExistingVersion() {
        return super.getExistingVersionInternal();
    }

    /**
     * Returns the existing row value if available. This value will
     * only be available if the conditional put operation failed and the request
     * specified that return information be returned using
     * {@link PutRequest#setReturnRow}.
     *
     * @return the value
     */
    public MapValue getExistingValue() {
        return super.getExistingValueInternal();
    }

    /* from Result */

    /**
     * Returns the read throughput consumed by this operation, in KBytes.
     * This is the actual amount of data read by the operation. The number
     * of read units consumed is returned by {@link #getReadUnits} which may
     * be a larger number because this was an update operation.
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
     * This number may be larger than that returned by {@link #getReadKB}
     * because it was an update operation.
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

    /**
     * Returns the value generated if the operation created a new value. This
     * can happen if the table contains an identity column or string column
     * declared as a generated UUID. If the table has no such columns this
     * value is null. If a value was generated for the operation, it is
     * non-null.
     *
     * @return the generated value
     *
     * @since 5.0.1
     */
    public FieldValue getGeneratedValue() {
        return generatedValue;
    }

    /**
     * @hidden
     * @param value the value
     * @return this
     */
    public PutResult setGeneratedValue(FieldValue value) {
        this.generatedValue = value;
        return this;
    }

    @Override
    public String toString() {
        return (version != null) ? version.toString() : "null Version";
    }
}
