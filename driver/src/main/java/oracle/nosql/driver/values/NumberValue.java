/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.values;

import static oracle.nosql.driver.util.CheckNull.requireNonNull;

import java.math.BigDecimal;
import java.util.Arrays;

import oracle.nosql.driver.util.NumberUtil;
import oracle.nosql.driver.util.SizeOf;

/**
 * A {@link FieldValue} instance representing an arbitrary-precision numeric
 * value.
 */
public class NumberValue extends FieldValue {

    /* The byte array that represents the Number value */
    private byte[] value;

    /**
     * Creates a new instance.
     *
     * @param value the value to use
     */
    public NumberValue(BigDecimal value) {
        super();
        requireNonNull(value, "NumberValue: value must be non-null");
        this.value = NumberUtil.serialize(value);
    }

    /**
     * Creates a new instance from a String value
     *
     * @param value the value to use
     *
     * @throws NumberFormatException if the value is not a valid BigDecimal
     */
    public NumberValue(String value) {
        super();
        this.value = NumberUtil.serialize(new BigDecimal(value));
    }

    @Override
    public Type getType() {
        return Type.NUMBER;
    }

    /**
     * @hidden
     * Internal use only
     *
     * Creates a new instance.
     *
     * @param value the byte array that represents the Number value.
     */
    public NumberValue(byte[] value) {
        super();
        requireNonNull(value, "NumberValue: value must be non-null");
        this.value = value;
    }

    /**
     * Returns the number value of this object
     *
     * @return the number value
     */
    public BigDecimal getValue() {
        return NumberUtil.deserialize(value);
    }

    /**
     * @hidden
     *
     * @param v the value to use
     */
    public void setValue(BigDecimal v) {
        value = NumberUtil.serialize(v);;
    }

    /**
     * Casts this number to a double, possibly with loss of information about
     * magnitude, precision or sign.
     *
     * @return a double value
     */
    @Override
    public double castAsDouble() {
         return NumberUtil.deserialize(value).doubleValue();
    }

    @Override
    public int compareTo(FieldValue other) {

        requireNonNull(other, "NumberValue.compareTo: other must be non-null");

        if (other instanceof NumberValue) {
            return compareUnsignedBytes(value, ((NumberValue)other).value);
        }

        BigDecimal otherVal = null;
        switch (other.getType()) {
        case INTEGER:
            otherVal = BigDecimal.valueOf(other.getInt());
            break;
        case LONG:
            otherVal = BigDecimal.valueOf(other.getLong());
            break;
        case DOUBLE:
            /*
             * Parse from String representation, to avoid rounding errors that
             * non-decimal floating operations could incur
             */
            otherVal = BigDecimal.valueOf(other.getDouble());
            break;
        default:
            throw new ClassCastException("Object is not a numeric type");
        }
        return getValue().compareTo(otherVal);
    }

    @Override
    public String getString() {
        return toJson(null);
    }

    @Override
    public String toJson(JsonOptions options) {
        return getValue().toString();
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof NumberValue) {
            return Arrays.equals(value, ((NumberValue) other).value);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return value.hashCode();
    }

    /**
     * @hidden
     * Internal use only.
     *
     * Returns the byte array that represents this Number value.
     * @return the byte[] value
     */
    public byte[] getBytes() {
        return value;
    }

    /**
     * This is directly from JE's com.sleepycat.je.tree.Key class and is the
     * default byte comparator for JE's btree.
     *
     * Compare using a default unsigned byte comparison.
     */
    private static int compareUnsignedBytes(byte[] key1,
                                            int off1,
                                            int len1,
                                            byte[] key2,
                                            int off2,
                                            int len2) {
        int limit = Math.min(len1, len2);

        for (int i = 0; i < limit; i++) {
            byte b1 = key1[i + off1];
            byte b2 = key2[i + off2];
            if (b1 == b2) {
                continue;
            }
            /*
             * Remember, bytes are signed, so convert to shorts so that we
             * effectively do an unsigned byte comparison.
             */
            return (b1 & 0xff) - (b2 & 0xff);
        }

        return (len1 - len2);
    }

    private static int compareUnsignedBytes(byte[] key1, byte[] key2) {
        return compareUnsignedBytes(key1, 0, key1.length, key2, 0, key2.length);
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {
        return (SizeOf.OBJECT_OVERHEAD +
                SizeOf.OBJECT_REF_OVERHEAD +
                SizeOf.byteArraySize(value.length));
    }
}
