/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static oracle.nosql.driver.util.CheckNull.requireNonNull;

import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.values.FieldValue;

/**
 * FieldRange defines a range of values to be used in a
 * {@link NoSQLHandle#multiDelete} operation, as specified in
 * {@link MultiDeleteRequest#setRange}.
 * FieldRange is used as the least significant component in a partially
 * specified key value in order to create a value range for an operation that
 * returns multiple rows or keys. The data types supported by FieldRange are
 * limited to the atomic types which are valid for primary keys.
 * <p>
 * The <i>least significant component</i> of a key is the first component of the
 * key that is not fully specified. For example, if the primary key for a
 * table is defined as the tuple &lt;a, b, c&gt; a FieldRange can be specified
 * for <em>a</em> if the primary key supplied is empty. A FieldRange can be
 * specified for <em>b</em> if the primary key supplied to the operation
 * has a concrete value for <em>a</em> but not for <em>b</em> or <em>c</em>.
 * </p>
 * <p>
 * This object is used to scope a {@link NoSQLHandle#multiDelete} operation.
 * The fieldPath specified must name a field in a table's primary key.
 * The values used must be of the same type and that type must match
 * the type of the field specified.
 * </p>
 * <p>
 * Validation of this object is performed when is it
 * used in an operation. Validation includes verifying that the field
 * is in the required key and, in the case of a composite key, that the field
 * is in the proper order relative to the key used in the operation.
 */
public class FieldRange {

    private final String fieldPath;
    private FieldValue start;
    private boolean startInclusive;
    private FieldValue end;
    private boolean endInclusive;

    /**
     * Create a value based on a specific field
     *
     * @param fieldPath the path to the field used in the range
     */
    public FieldRange(String fieldPath) {
        requireNonNull(fieldPath,
                       "FieldRange: fieldPath must be non-null");
        this.fieldPath = fieldPath;
    }

    /**
     * Returns the FieldValue that defines lower bound of the range,
     * or null if no lower bound is enforced.
     *
     * @return the start FieldValue
     */
    public FieldValue getStart() {
        return start;
    }

    /**
     * Returns whether start is included in the range, i.e., start is less than
     * or equal to the first FieldValue in the range.  This value is valid only
     * if the start value is not null.
     *
     * @return true if the start value is inclusive
     */
    public boolean getStartInclusive() {
        return startInclusive;
    }

    /**
     * Returns the FieldValue that defines upper bound of the range,
     * or null if no upper bound is enforced.
     *
     * @return the end FieldValue
     */
    public FieldValue getEnd() {
        return end;
    }

    /**
     * Returns whether end is included in the range, i.e., end is greater than
     * or equal to the last FieldValue in the range.  This value is valid only
     * if the end value is not null.
     *
     * @return true if the end value is inclusive
     */
    public boolean getEndInclusive() {
        return endInclusive;
    }

    /**
     * Returns the name for the field used in the range.
     *
     * @return the name of the field
     */
    public String getFieldPath() {
        return fieldPath;
    }

    /**
     * Sets the start value of the range to the specified value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     */
    public FieldRange setStart(FieldValue value, boolean isInclusive) {
        requireNonNull(value,
                       "FieldRange.setStart: value must be non-null");

        start = value;
        startInclusive = isInclusive;
        return this;
    }

    /**
     * Sets the end value of the range to the specified value.
     *
     * @param value the value to set
     *
     * @param isInclusive set to true if the range is inclusive of the value,
     * false if it is exclusive.
     *
     * @return this
     */
    public FieldRange setEnd(FieldValue value, boolean isInclusive) {
        requireNonNull(value,
                       "FieldRange.setEnd: value must be non-null");

        end = value;
        endInclusive = isInclusive;
        return this;
    }

    /**
     * Internal use only
     *
     * Ensures that the object is self-consistent and if not, throws
     * IllegalArgumentException. Validation of the range values themselves
     * is done remotely.
     */
    public void validate() {
        FieldValue.Type startType = (start == null ? null : start.getType());
        FieldValue.Type endType = (end == null ? null : end.getType());
        if (startType == null && endType == null) {
            throw new IllegalArgumentException(
                "FieldRange: must specify a start or end value");
        }
        if (startType != null && !(endType == null || endType == startType)) {
            throw new IllegalArgumentException(
                "FieldRange: Mismatch of start and end types. Start type " +
                "is " + startType + ", end type is " + endType);
        }
        if (endType != null && !(startType == null || endType == startType)) {
            throw new IllegalArgumentException(
                "FieldRange: Mismatch of start and end types. Start type " +
                "is " + startType + ", end type is " + endType);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("{");
        sb.append(" \"Path\" : ").append(fieldPath);
        sb.append(", \"Start\" : ").append(start != null ? start : "null");
        sb.append(", \"End\" : ").append(end != null ? end : "null");
        sb.append(", \"StartInclusive\" : ").append(startInclusive);
        sb.append(", \"EndInclusive\" : ").append(endInclusive);
        sb.append(" }");
        return sb.toString();
    }
}
