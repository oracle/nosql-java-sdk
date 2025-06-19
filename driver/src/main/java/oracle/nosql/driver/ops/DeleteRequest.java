/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.Durability;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;

/**
 * Represents the input to a {@link NoSQLHandle#delete} operation.
 *
 * This request can be used to perform unconditional and conditional deletes:
 * <ul>
 * <li>Delete any existing row. This is the default.</li>
 * <li>Succeed only if the row exists and its {@link Version} matches a specific
 * {@link Version}. Use {@link #setMatchVersion} for this case.
 * Using this option in conjunction with using {@link #setReturnRow} allows
 * information about the existing row to be returned if the operation fails
 * because of a version mismatch. On success no information is returned.
 * Using {@link #setReturnRow} may incur additional cost and affect operation
 * latency.</li>
 * </ul>
 * <p>
 * The table name and key are required parameters.
 * On a successful operation {@link DeleteResult#getSuccess} returns true.
 * Additional information, such as previous row information, may be available
 * in {@link DeleteResult}.
 * @see NoSQLHandle#delete
 */
public class DeleteRequest extends WriteRequest {
    private MapValue key;
    private Version matchVersion;

    /**
     * Constructs an empty request
     */
    public DeleteRequest() {}

    /**
     * Returns the key of the row to be deleted. This is a required
     * field.
     *
     * @return the key value, or null if not set
     */
    public MapValue getKey() {
        return key;
    }

    /**
     * Sets the key to use for the delete operation. This is a required
     * field.
     *
     * @param key the key value
     *
     * @return this
     */
    public DeleteRequest setKey(MapValue key) {
        this.key = key;
        return this;
    }

    /**
     * Sets the key to use for the delete operation based on a JSON string.
     * The string is parsed for validity and stored internally as a
     * {@link MapValue}.
     *
     * @param jsonValue the key value as a JSON string
     *
     * @param options optional configuration to specify how to map JSON
     * data, may be null
     *
     * @return this
     */
    public DeleteRequest setKeyFromJson(String jsonValue,
                                        JsonOptions options) {
        if (jsonValue == null) {
            throw new IllegalArgumentException(
                "DeleteRequest: setValueFromJson requires a non-null value");
        }
        key = (MapValue) JsonUtils.createValueFromJson(jsonValue, options);
        return this;
    }

    /**
     * Returns the {@link Version} used for a match on a conditional delete.
     *
     * @return the Version or null if not set
     */
    public Version getMatchVersion() {
        return matchVersion;
    }

    /**
     * Sets the {@link Version} to use for a conditional delete operation.
     * The Version is usually obtained from {@link GetResult#getVersion} or
     * other method that returns a Version. When set, the delete operation will
     * succeed only if the row exists and its Version matches the one
     * specified. Using this option will incur additional cost.
     *
     * @param version the Version to match
     *
     * @return this
     */
    public DeleteRequest setMatchVersion(Version version) {
        this.matchVersion = version;
        return this;
    }

    /* getters for WriteRequest and Request fields */

    /**
     * Returns the timeout to use for the operation, in milliseconds. A value
     * of 0 indicates that the timeout has not been set.
     *
     * @return the value
     */
    public int getTimeout() {
        return super.getTimeoutInternal();
    }

    /**
     * Returns whether information about the existing row should be returned.
     * See {@link DeleteRequest#setReturnRow} for details about what information
     * is returned.
     *
     * @return true if information should be returned.
     */
    public boolean getReturnRow() {
        return super.getReturnRowInternal();
    }

    /**
     * Sets the table name to use for the operation. This is a required
     * parameter.
     *
     * @param tableName the table name
     *
     * @return this
     */
    public DeleteRequest setTableName(String tableName) {
        super.setTableNameInternal(tableName);
        return this;
    }

    /**
     * Sets the durability to use for the operation.
     * On-premises only.
     *
     * @param durability the durability value
     *
     * @return this
     *
     * @since 5.3.0
     */
    public DeleteRequest setDurability(Durability durability) {
        setDurabilityInternal(durability);
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
    public DeleteRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Sets whether information about the existing row should be returned.
     * The existing row information, including the value, version, and
     * modification time, will only be returned if
     * {@link DeleteRequest#setReturnRow} is true and one of the following
     * occurs:
     * <ul>
     * <li> The {@link DeleteRequest#setMatchVersion} is used and the operation
     * fails because the row exists and its version does not match.
     * </li>
     * <li> The {@link DeleteRequest#setMatchVersion} is not used and the
     * operation succeeds provided that the server supports providing the
     * existing row.
     * </li>
     * </ul>
     * This parameter is optional and defaults
     * to false. It's use may incur additional cost.
     *
     * @param value set to true if information should be returned
     *
     * @return this
     */
    public DeleteRequest setReturnRow(boolean value) {
        super.setReturnRowInternal(value);
        return this;
    }

    /**
     * Sets the optional request timeout value, in milliseconds. This overrides
     * any default value set with {@link NoSQLHandleConfig#setRequestTimeout}.
     * The value must be positive.
     *
     * @param timeoutMs the timeout value, in milliseconds
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is less than
     * or equal to 0
     */
    public DeleteRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /**
     * Sets the optional namespace.
     * On-premises only.
     *
     * This overrides any default value set with
     * {@link NoSQLHandleConfig#setDefaultNamespace}.
     * Note: if a namespace is specified in the table name for the request
     * (using the namespace:tablename format), that value will override this
     * setting.
     *
     * @param namespace the namespace to use for the operation
     *
     * @return this
     *
     * @since 5.4.10
     */
    public DeleteRequest setNamespace(String namespace) {
        super.setNamespaceInternal(namespace);
        return this;
    }

    /**
     * Sets the row metadata to use for this request. This is an optional
     * parameter.<p>
     *
     * Row metadata is associated to a certain version of a row. Any subsequent
     * write operation will use its own row metadata value. If not specified
     * null will be used by default.<p>
     *
     * The @parameter rowMetadata must be null or a valid JSON construct:
     * object, array, string, number, true, false or null, otherwise an
     * IllegalArgumentException is thrown.
     *
     * @param rowMetadata the row metadata
     * @throws IllegalArgumentException if rowMetadata not null and invalid
     * JSON construct
     *
     * @since 5.4.18
     * @return this
     */
    public DeleteRequest setRowMetadata(String rowMetadata) {
        super.setRowMetadata(rowMetadata);
        return this;
    }

    /**
     * @hidden
     */
    @Override
    public void validate() {
        validateWriteRequest("DeleteRequest");
        if (key == null) {
            throw new IllegalArgumentException(
                "DeleteRequest requires a key");
        }
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createDeleteSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createDeleteDeserializer();
    }

    @Override
    public String getTypeName() {
        return "Delete";
    }

    /**
     * @hidden
     */
    @Override
    public boolean doesReads() {
        return (matchVersion != null || getReturnRow());
    }
}
