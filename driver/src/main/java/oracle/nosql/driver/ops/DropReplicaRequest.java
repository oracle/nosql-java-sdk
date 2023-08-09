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
 * DropReplicaRequest is used to drop a replica region on a table.
 *
 * @since 5.4.13
 */
public class DropReplicaRequest extends Request {

    private String region;

    /**
     * Sets the table name to use for the operation.
     *
     * @param tableName the name
     *
     * @return this
     */
    public DropReplicaRequest setTableName(String tableName) {
        super.setTableNameInternal(tableName);
        return this;
    }

    /**
     * Sets the region name to be dropped
     *
     * @param region the region name
     *
     * @return this
     */
    public DropReplicaRequest setRegion(String region) {
        this.region = region;
        return this;
    }

    /**
     * Returns the region name to be dropped
     *
     * @return the region name
     */
    public String getRegion() {
        return region;
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
    public DropReplicaRequest setCompartment(String compartment) {
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
    public DropReplicaRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    @Override
    public void validate() {
        if (tableName == null || region == null) {
            throw new IllegalArgumentException(
                "DropReplicaRequest requires table name and region");
        }
    }

    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createDropReplicaSerializer();
    }

    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createDropReplicaDeserializer();
    }

    @Override
    public String getTypeName() {
        return "DropReplica";
    }
}
