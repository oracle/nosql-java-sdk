/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.values;

import static oracle.nosql.driver.util.CheckNull.requireNonNull;

import java.math.BigDecimal;

import oracle.nosql.driver.util.SizeOf;

/**
 * A {@link FieldValue} instance representing a double value.
 */
public class DoubleValue extends FieldValue {

    private double value;

    /**
     * Creates a new instance.
     *
     * @param value the value to use
     */
    public DoubleValue(double value) {
        super();
        this.value = value;
    }

    /**
     * Creates a new instance from a String value
     *
     * @param value the value to use
     *
     * @throws NumberFormatException if the value is not a valid double
     */
    public DoubleValue(String value) {
        super();
        this.value = Double.parseDouble(value);
    }

    @Override
    public Type getType() {
        return Type.DOUBLE;
    }

    /**
     * Returns the double value of this object
     *
     * @return the double value
     */
    public double getValue() {
        return value;
    }

    /**
     * internal use only
     *
     * @param v the value to use
     * @hidden
     */
    public void setValue(double v) {
        value = v;
    }

    /**
     * Returns the double value of this object
     *
     * @return the double value
     */
    @Override
    public double castAsDouble() {
        return value;
    }

    @Override
    public int compareTo(FieldValue other) {
        requireNonNull(other, "DoubleValue.compareTo: other must be non-null");
        return Double.compare(value, other.getDouble());
    }

    /**
     * Returns a BigDecimal value for this object
     *
     * @return the BigDecimal value
     */
    @Override
    public BigDecimal getNumber() {
        return new BigDecimal(value);
    }

    @Override
    public String getString() {
        return toJson(null);
    }

    @Override
    public String toJson(JsonOptions options) {
        return String.valueOf(value);
    }

    @Override
    public boolean equals(Object other) {
        if (other instanceof DoubleValue) {
            /* Use Double.equals to handle values like NaN */
            return ((Double)value).equals(((DoubleValue)other).value);
        }
        return false;
    }

    @Override
    public int hashCode() {
        return ((Double) value).hashCode();
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {
        return SizeOf.OBJECT_OVERHEAD + 8;
    }
}
