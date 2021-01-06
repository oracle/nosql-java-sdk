/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;

/**
 * Represents the input to a {@link NoSQLHandle#get} operation which returns a
 * single row based on the specified key.
 * @see NoSQLHandle#get
 */
public class GetRequest extends ReadRequest {
    private MapValue key;

    /**
     * Returns the primary key used for the operation. This is a required
     * parameter.
     *
     * @return the key
     */
    public MapValue getKey() {
        return key;
    }

    /**
     * Sets the primary key used for the get operation. This is a required
     * parameter.
     *
     * @param key the primary key
     *
     * @return this
     */
    public GetRequest setKey(MapValue key) {
        this.key = key;
        return this;
    }

     /**
     * Sets the key to use for the get operation based on a JSON string. This is
     * a convenience method equivalent to:
     * <pre>
     * setKey(FieldValue.createFromJson(jsonValue, null));
     * </pre>
     *
     * @param jsonValue the row value as a JSON string
     *
     * @param options optional configuration to specify how to map JSON
     * data, may be null
     *
     * @return this
     */
    public GetRequest setKeyFromJson(String jsonValue,
                                     JsonOptions options) {
        /*
         * TODO: use the JSON string intact and parse on serialization,
         * avoiding MapValue
         */
        if (jsonValue == null) {
            throw new IllegalArgumentException(
                "GetRequest: setKeyFromJson requires a non-null value");
        }
        this.key = (MapValue) JsonUtils.createValueFromJson(jsonValue, options);
        return this;
    }

    /* setters for ReadRequest */

    /**
     * Sets the table name to use for the operation. This is a required
     * parameter.
     *
     * @param tableName the table name
     *
     * @return this
     */
    public GetRequest setTableName(String tableName) {
        super.setTableNameInternal(tableName);
        return this;
    }

    /**
     * Cloud service only.
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
    public GetRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Sets the {@link Consistency} to use for the operation. This parameter is
     * optional and if not set the default consistency configured for the
     * {@link NoSQLHandle} is used.
     *
     * @param consistency the Consistency
     *
     * @return this
     */
    public GetRequest setConsistency(Consistency consistency) {
        super.setConsistencyInternal(consistency);
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
    public GetRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /**
     * @hidden
     * Internal use only
     *
     * Validates the state of the members of this class for use.
     *
     * @throws IllegalArgumentException if state is invalid
     */
    @Override
    public void validate() {
        validateReadRequest("GetRequest");
        if (key == null) {
            throw new IllegalArgumentException(
                "GetRequest requires a key value");
        }
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createGetSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createGetDeserializer();
    }
}
