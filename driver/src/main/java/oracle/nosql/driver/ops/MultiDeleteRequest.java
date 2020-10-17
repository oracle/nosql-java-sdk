/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
package oracle.nosql.driver.ops;

import oracle.nosql.driver.FieldRange;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.values.MapValue;

/**
 * Represents the input to a {@link NoSQLHandle#multiDelete} operation which can
 * be used to delete a range of values that match the primary key and range
 * provided.
 * <p>
 * A range is specified using a partial key plus a range based on the
 * portion of the key that is not provided. For example if a table's primary key
 * is &lt;id, timestamp&gt; and the its shard key is the id, it is possible
 * to delete a range of timestamp values for a specific id by providing an id
 * but no timestamp in the value used for {@link #setKey} and providing a range
 * of timestamp values in the {@link FieldRange} used in {@link #setRange}.
 * <p>
 * Because this operation can exceed the maximum amount of data modified in a
 * single operation a continuation key can be used to continue the operation.
 * The continuation key is obtained from
 * {@link MultiDeleteResult#getContinuationKey} and set in a new request using
 * {@link MultiDeleteRequest#setContinuationKey}. Operations with a continuation
 * key still require the primary key.
 * @see NoSQLHandle#multiDelete
 */
public class MultiDeleteRequest extends Request {

    private MapValue key;
    private byte[] continuationKey;
    private FieldRange range;
    private int maxWriteKB;

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
    public MultiDeleteRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Sets the table name to use for the operation. This is a required
     * parameter.
     *
     * @param tableName the table name
     *
     * @return this
     */
    public MultiDeleteRequest setTableName(String tableName) {
        super.setTableNameInternal(tableName);
        return this;
    }

    /**
     * Returns the key to be used for the operation.
     *
     * @return the key
     */
    public MapValue getKey() {
        return key;
    }

    /**
     * Sets the key to be used for the operation. This is a required
     * parameter and must completely specify the target table's shard
     * key.
     *
     * @param key the key
     *
     * @return this
     */
    public MultiDeleteRequest setKey(MapValue key) {
        this.key = key;
        return this;
    }

    /**
     * Returns the {@link FieldRange} to be used for the operation if set.
     *
     * @return the range, null if no range is to be used
     */
   public FieldRange getRange() {
        return range;
    }

    /**
     * Sets the {@link FieldRange} to be used for the operation. This parameter
     * is optional, but required to delete a specific range of rows.
     *
     * @param range the range
     *
     * @return this
     */
    public MultiDeleteRequest setRange(FieldRange range) {
        this.range = range;
        return this;
    }

    /**
     * Returns the limit on the total KB write during this operation. If
     * not set by the application this value will be 0 which means the
     * default system limit is used.
     *
     * @return the limit, or 0 if not set
     */
    public int getMaxWriteKB() {
        return maxWriteKB;
    }

    /**
     * Sets the limit on the total KB write during this operation, 0 means no
     * application-defined limit. This value can only reduce the system defined
     * limit.
     *
     * @param maxWriteKB the limit in terms of number of KB write during this
     * operation.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the maxWriteKB value is less than 0.
     */
    public MultiDeleteRequest setMaxWriteKB(int maxWriteKB) {
        if (maxWriteKB < 0) {
            throw new IllegalArgumentException("maxWriteKB must be >= 0");
        }
        this.maxWriteKB = maxWriteKB;
        return this;
    }

    /**
     * Returns the continuation key if set.
     *
     * @return the continuation key
     */
    public byte[] getContinuationKey() {
        return continuationKey;
    }

    /**
     * Sets the continuation key.
     *
     * @param continuationKey the key which should have been obtained from
     * {@link MultiDeleteResult#getContinuationKey}
     *
     * @return this;
     */
    public MultiDeleteRequest setContinuationKey(byte[] continuationKey) {
        this.continuationKey = continuationKey;
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
    public MultiDeleteRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createMultiDeleteSerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createMultiDeleteDeserializer();
    }

    /**
     * @hidden
     */
    @Override
    public void validate() {
        if (tableName == null) {
            throw new IllegalArgumentException(
            ("MultiDeleteRequest requires table name"));
        }
        if (key == null) {
            throw new IllegalArgumentException
            ("MultiDeleteRequest requires a key");
        }
        if (range != null) {
            range.validate();
        }
    }

    /**
     * @hidden
     */
    @Override
    public boolean doesReads() {
        return true;
    }
}
