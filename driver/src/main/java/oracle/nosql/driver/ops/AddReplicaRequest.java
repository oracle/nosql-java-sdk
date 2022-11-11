/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
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
 * AddReplicaRequest is used to add new replica region on a table.
 */
public class AddReplicaRequest extends Request {

    private String region;
    private int readUnits;
    private int writeUnits;

    /**
     * Sets the table name to use for the operation.
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
     * Sets the new replica region to be added.
     *
     * @param region the name of region
     *
     * @return this
     */
    public AddReplicaRequest setRegion(String region) {
        this.region = region;
        return this;
    }

    /**
     * Returns the new region name
     *
     * @return the region name
     */
    public String getRegion() {
        return region;
    }

    /**
     * Sets the read units for the table on new region
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
     * Returns the read units of the table on new region
     *
     * @return the read units
     */
    public int getReadUnits() {
        return readUnits;
    }

    /**
     * Sets the write units for the table on new region
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
     * Returns the write units of the table on new region
     *
     * @return the write units
     */
    public int getWriteUnits() {
        return writeUnits;
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

    @Override
    public void validate() {
        if (region == null || tableName == null) {
            throw new IllegalArgumentException(
                "AddReplicaRequest requires table name and region");
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
        return "Table";
    }
}
