/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import java.util.Arrays;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PutRequest;

/**
 * Version is an opaque class that represents the version of a row in the
 * database. It is returned by successful {@link NoSQLHandle#get} operations
 * and can be used in {@link PutRequest#setMatchVersion} and
 * {@link DeleteRequest#setMatchVersion} to conditionally perform those
 * operations to ensure an atomic read-modify-write cycle. This is an opaque
 * object from an application perspective.
 * <p>
 * Use of Version in this way adds cost to operations so it should be done only
 * if necessary
 */
public class Version {
    private final byte[] version;

    private Version(byte[] version) {
        this.version = version;
    }

    /**
     * @hidden
     * @return the version bytes
     */
    public byte[] getBytes() {
        return version;
    }

    /**
     * Creates a Version instance from a byte[] which may have been acquired
     * from a query using the row_version() function which returns a FieldValue
     * of type BINARY.
     *
     * @param version the version to use
     * @return a new Version instance
     */
    public static Version createVersion(byte[] version) {
        if (version == null) {
            return null;
        }
        return new Version(version);
    }

    /**
     * @hidden
     */
    @Override
    public String toString() {
        if (version != null) {
            return "non-null Version";
        }
        return "null Version";
    }

    /**
     * @hidden
     */
    @Override
    public boolean equals(Object other) {
        if (other instanceof Version) {
            return Arrays.equals(version, ((Version)other).version);
        }
        return false;
    }

    /**
     * @hidden
     */
    @Override
    public int hashCode() {
        return Arrays.hashCode(version);
    }
}
