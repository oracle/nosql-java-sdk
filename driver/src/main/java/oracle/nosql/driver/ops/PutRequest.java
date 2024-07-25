/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.Durability;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.Version;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.MapValue;

/**
 * Represents the input to a {@link NoSQLHandle#put} operation.
 *
 * This request can be used to perform unconditional and conditional puts:
 * <ul>
 * <li>Overwrite any existing row. This is the default.</li>
 * <li>Succeed only if the row does not exist. Use {@link Option#IfAbsent} for
 * this case.</li>
 * <li>Succeed only if the row exists. Use {@link Option#IfPresent} for this
 * case</li>
 * <li>Succeed only if the row exists and its {@link Version} matches a specific
 * {@link Version}. Use {@link Option#IfVersion} for this case and
 * {@link #setMatchVersion} to specify the version to match</li>
 * </ul>
 * <p>
 * Information about the existing row can be returned on failure of a put
 * operation using {@link Option#IfVersion} or {@link Option#IfAbsent} by
 * using {@link #setReturnRow}. Requesting this information incurs
 * additional cost and may affect operation latency.
 * <p>
 * On a successful operation the {@link Version} returned
 * by {@link PutResult#getVersion} is non-null. Additional information,
 * such as previous row information, may be available in {@link PutResult}.
 * @see NoSQLHandle#put
 */
public class PutRequest extends WriteRequest {
    private MapValue value;
    private Option option;
    private Version matchVersion;
    private TimeToLive ttl;
    private boolean updateTTL;
    private boolean exactMatch;
    private int identityCacheSize;

    /**
     * Specifies a condition for the put operation.
     */
    public enum Option {
        /**
         * Put should only succeed if the row does not exist.
         */
        IfAbsent,

        /**
         * Put should only succeed if the row exists.
         */
        IfPresent,

        /**
         * Put should succeed only if the row exists and its Version matches
         * the one specified match version using {@link #setMatchVersion}
         */
        IfVersion
    }

    /**
     * Constructs an empty request
     */
    public PutRequest() {}

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
    public PutRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Returns the value of the row to be used.
     *
     * @return the value, or null if not set
     */
    public MapValue getValue() {
        return value;
    }

    /**
     * Sets the value to use for the put operation.
     * This is a required parameter and must be set using this method or
     * {@link #setValueFromJson}.
     *
     * @param value the row value
     *
     * @return this
     */
    public PutRequest setValue(MapValue value) {
        this.value = value;
        return this;
    }

     /**
     * Sets the value to use for the put operation based on a JSON string.
     * The string is parsed for validity and stored internally as a
     * {@link MapValue}.
     * This is a required parameter and must be set using this method or
     * {@link #setValue}.
     *
     * @param jsonValue the row value as a JSON string
     *
     * @param options optional configuration to specify how to map JSON
     * data
     *
     * @return this
     */
    public PutRequest setValueFromJson(String jsonValue, JsonOptions options) {
        /*
         * TODO: use the JSON string intact and parse on serialization,
         * avoiding MapValue
         */
        if (jsonValue == null) {
            throw new IllegalArgumentException(
                "PutRequest: setValueFromJson requires a non-null value");
        }
        this.value = (MapValue) JsonUtils.createValueFromJson(jsonValue,
                                                              options);
        return this;
    }

    /**
     * Returns the {@link TimeToLive} value, if set.
     *
     * @return the {@link TimeToLive} if set, null otherwise.
     */
    public TimeToLive getTTL() {
        return ttl;
    }

    /**
     * Sets the {@link TimeToLive} value, causing the time to live on
     * the row to be set to the specified value on put. This value overrides any
     * default time to live setting on the table.
     *
     * @param ttl the time to live
     *
     * @return this
     */
    public PutRequest setTTL(TimeToLive ttl) {
        this.ttl = ttl;
        return this;
    }

    /**
     * If true, and there is an existing row, causes the operation to update
     * the time to live (TTL) value of the row based on the Table's default
     * TTL if set. If the table has no default TTL this state has no effect.
     * By default updating an existing row has no effect on its TTL.
     *
     * @param value true or false
     *
     * @return this
     */
    public PutRequest setUseTableDefaultTTL(boolean value) {
        updateTTL = value;
        return this;
    }

    /**
     * Returns whether or not to update the row's time to live (TTL) based on
     * a table default value if the row exists. By default updates of existing
     * rows do not affect that row's TTL.
     *
     * @return the value
     */
    public boolean getUseTableDefaultTTL() {
        return updateTTL;
    }

    /**
     * Returns the {@link Version} used for a match on a conditional put.
     *
     * @return the Version or null if not set
     */
    public Version getMatchVersion() {
        return matchVersion;
    }

    /**
     * Sets the {@link Version} to use for a conditional put operation.
     * The Version is usually obtained from {@link GetResult#getVersion} or
     * other method that returns a Version. When set, the put operation will
     * succeed only if the row exists and its Version matches the one
     * specified. This condition exists to allow an application to ensure
     * that it is updating a row in an atomic read-modify-write cycle.
     * Using this mechanism incurs additional cost.
     *
     * @param version the Version to match
     *
     * @return this
     */
    public PutRequest setMatchVersion(Version version) {
        if (option == null) {
            option = Option.IfVersion;
        }
        this.matchVersion = version;
        return this;
    }

    /**
     * Returns the option specified for the put.
     *
     * @return the option specified.
     */
    public Option getOption() {
        return option;
    }

    /**
     * Sets the option for the put.
     *
     * @param option the option to set.
     *
     * @return this
     */
    public PutRequest setOption(Option option) {
        this.option = option;
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
     * See {@link PutRequest#setReturnRow} for details on the return
     * information.
     *
     * @return true if information should be returned.
     */
    public boolean getReturnRow() {
        return super.getReturnRowInternal();
    }

    /* setters for WriteRequest fields */

    /**
     * Sets the table name to use for the operation
     *
     * @param tableName the table name
     *
     * @return this
     */
    public PutRequest setTableName(String tableName) {
        super.setTableNameInternal(tableName);
        return this;
    }

    /**
     * Sets whether information about the existing row should be returned.
     * The existing row information, including the value, Version, and
     * modification time, will only be returned if
     * {@link PutRequest#setReturnRow} is true and one of the following occurs:
     * <ul>
     * <li>The {@link Option#IfAbsent} is used and the operation fails because
     * the row already exists.</li>
     * <li>The {@link Option#IfVersion} is used and the operation fails because
     * the row exists and its version does not match.
     * </li>
     * <li>The {@link Option#IfPresent} is used and the operation succeeds
     * provided that the server supports providing the existing row.
     * </li>
     * <li>The {@link Option} is not used and put operation replaces the
     * existing row provided that the server supports providing the existing
     * row.
     * </li>
     * </ul>
     *
     * This setting is optional and defaults to false. If true the operation
     * will incur additional cost.
     *
     * @param value set to true if information should be returned
     *
     * @return this
     */
    public PutRequest setReturnRow(boolean value) {
        super.setReturnRowInternal(value);
        return this;
    }

    /**
     * Sets the durability to use for the operation.
     * On-premises only.
     *
     * @param durability the durability value. Set to null for
     * the default durability setting on the server.
     *
     * @return this
     *
     * @since 5.3.0
     */
    public PutRequest setDurability(Durability durability) {
        setDurabilityInternal(durability);
        return this;
    }

    /**
     * Sets the request timeout value, in milliseconds. This overrides any
     * default value set in with {@link NoSQLHandleConfig#setRequestTimeout}.
     * The value must be positive.
     *
     * @param timeoutMs the timeout value, in milliseconds
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is less than
     * or equal to 0
     */
    public PutRequest setTimeout(int timeoutMs) {
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
    public PutRequest setNamespace(String namespace) {
        super.setNamespaceInternal(namespace);
        return this;
    }

    /**
     * If true the value must be an exact match for the table schema or the
     * operation will fail. An exact match means that there are no required
     * fields missing and that there are no extra, unknown fields. The default
     * behavior is to not require an exact match.
     *
     * @param value true or false
     *
     * @return this
     *
     * @since 5.0.1
     */
    public PutRequest setExactMatch(boolean value) {
        exactMatch = value;
        return this;
    }

    /**
     * Returns whether or not the value must be an exact match to the table
     * schema or not.
     *
     * @return the value
     *
     * @since 5.0.1
     */
    public boolean getExactMatch() {
        return exactMatch;
    }

    /**
     * Sets the number of generated identity values that are requested from
     * the server during a put. This takes precedence over the DDL identity
     * CACHE option set during creation of the identity column.
     *
     * Any value equal or less than 0 means that the DDL identity CACHE value
     * is used.
     *
     * @param size the size
     * @return this
     *
     * @since 5.0.1
     */
    public PutRequest setIdentityCacheSize(int size) {
        identityCacheSize = size;
        return this;
    }

    /**
     * Gets the number of generated identity values that are requested from
     * the server during a put if set in this request.
     *
     * @return the value, or 0 if it has not been set.
     *
     * @since 5.0.1
     */
    public int getIdentityCacheSize() {
        return identityCacheSize;
    }

    /**
     * @hidden
     * Internal use only
     *
     * Validates the state of the object when complete.
     *
     * @throws IllegalArgumentException if the state is incomplete or
     * invalid.
     */
    @Override
    public void validate() {
        validateWriteRequest("PutRequest");
        if (value == null) {
            throw new IllegalArgumentException(
                "PutRequest requires a value");
        }
        validateIfOptions();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createPutSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createPutDeserializer();
    }

    @Override
    public String getTypeName() {
        return "Put";
    }

    /**
     * @hidden
     *
     * Returns true if the operation should update the ttl.
     * @return true if the operation should update the ttl on the row if
     * it already exists
     */
    public boolean getUpdateTTL() {
        return(updateTTL || ttl != null);
    }

    /**
     * Ensures that only one of ifAbsent, ifPresent, or matchVersion is set
     */
    private void validateIfOptions() {
        if (option == Option.IfVersion && matchVersion == null) {
            throw new IllegalArgumentException(
                "PutRequest: matchVersion must be specified when " +
                "Options.IfVersion is used.");
        }
        if (option != Option.IfVersion && matchVersion != null) {
            throw new IllegalArgumentException(
                "PutRequest: matchVersion is specified, the option is not " +
                "Options.IfVersion but " + option);
        }

        if (updateTTL && ttl != null) {
            throw new IllegalArgumentException(
                "PutRequest: only one of setUseTableDefaultTTL or setTTL " +
                "may be specified");
        }
    }

    /**
     * @hidden
     */
    @Override
    public boolean doesReads() {
        return (option != null || getReturnRow());
    }
}
