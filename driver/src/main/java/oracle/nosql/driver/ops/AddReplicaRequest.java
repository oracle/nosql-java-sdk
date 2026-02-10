/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;

/**
 * Cloud service only.
 *
 * AddReplicaRequest is used to add new replica (region) to a table.
 *
 * @since 5.4.13
 */
public class AddReplicaRequest extends Request {

    private String replicaName;
    private int readUnits;
    private int writeUnits;
    private String matchETag;

    /**
     * Sets the table name to replicate
     *
     * @param tableName the name
     *
     * @return this
     */
    public AddReplicaRequest setTableName(String tableName) {
        super.setTableNameInternal(tableName);
        return this;
    }

    /**
     * Sets the replica name (region) to be added
     *
     * @param replicaName the name of the replica
     *
     * @return this
     */
    public AddReplicaRequest setReplicaName(String replicaName) {
        this.replicaName = replicaName;
        return this;
    }

    /**
     * Returns the replica name. This is the region name
     *
     * @return the replica name
     */
    public String getReplicaName() {
        return replicaName;
    }

    /**
     * Sets the read units for the replica table. This defaults
     * to the units on the existing table
     *
     * @param readUnits the read units
     *
     * @return this
     */
    public AddReplicaRequest setReadUnits(int readUnits) {
        this.readUnits = readUnits;
        return this;
    }

    /**
     * Returns the read units set for the replica table or 0 if not set
     *
     * @return the read units
     */
    public int getReadUnits() {
        return readUnits;
    }

    /**
     * Sets the write units for the replica table. This defaults
     * to the units on the existing table
     *
     * @param writeUnits the write units
     *
     * @return this
     */
    public AddReplicaRequest setWriteUnits(int writeUnits) {
        this.writeUnits = writeUnits;
        return this;
    }

    /**
     * Returns the write units set for the replica table or 0 if not set
     *
     * @return the write units
     */
    public int getWriteUnits() {
        return writeUnits;
    }

    /**
     * Sets an ETag in the request that must be matched for the operation
     * to proceed. The ETag must be non-null and have been returned in a
     * previous {@link TableResult}. This is a form of optimistic concurrency
     * control allowing an application to ensure no unexpected modifications
     * have been made to the table.
     *
     * @param etag the ETag
     *
     * @return this
     * @since 5.4.15
     */
    public AddReplicaRequest setMatchEtag(String etag) {
        this.matchETag = etag;
        return this;
    }

    /**
     * Returns the matchEtag, or null if not set
     *
     * @return the ETag
     * @since 5.4.15
     */
    public String getMatchETag() {
        return matchETag;
    }

    /**
     * <p>
     * Sets the name or id of a compartment to be used for this operation.
     * <p>
     * The compartment may be specified as either a name (or path for nested
     * compartments) or as an id (OCID). A name (vs id) can only
     * be used when authenticated using a specific user identity. It is
     * <b>not</b> available if authenticated as an Instance Principal which can
     * be done when calling the service from a compute instance in the Oracle
     * Cloud Infrastructure.  See {@link
     * SignatureProvider#createWithInstancePrincipal}
     *
     * @param compartment the name or id. If using a nested compartment,
     * specify the full compartment path
     * <code>compartmentA.compartmentB</code>, but exclude the name of the
     * root compartment (tenant).
     *
     * @return this
     */
    public AddReplicaRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Sets the request timeout value, in milliseconds. This overrides any
     * default value set in {@link NoSQLHandleConfig}. The value must be
     * positive.
     *
     * @param timeoutMs the timeout value, in milliseconds
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is less than
     * or equal to 0
     */
    public AddReplicaRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /*
     * use the default request timeout if not set.
     */
    @Override
    public AddReplicaRequest setDefaults(NoSQLHandleConfig config) {
        if (timeoutMs == 0) {
            timeoutMs = config.getDefaultTableRequestTimeout();
        }
        return this;
    }

    @Override
    public void validate() {
        if (tableName == null || replicaName == null) {
            throw new IllegalArgumentException(
                "AddReplicaRequest requires table name and replica name");
        }
    }

    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createAddReplicaSerializer();
    }

    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createAddReplicaDeserializer();
    }

    @Override
    public String getTypeName() {
        return "AddReplica";
    }
}
