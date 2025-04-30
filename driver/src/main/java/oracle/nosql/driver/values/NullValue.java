/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.values;

/**
 * A {@link FieldValue} instance representing a <i>null</i> or missing value
 * in a fully-typed schema. This type only exists in index keys on a fully-typed
 * field and never inside indexed JSON.
 */
public class NullValue extends FieldValue {

    private static final NullValue INSTANCE = new NullValue();

    private NullValue() {
        super();
    }

    @Override
    public Type getType() {
        return Type.NULL;
    }

    /**
     * Returns an instance (singleton) of NullValue.
     *
     * @return the value
     */
    public static NullValue getInstance() {
        return INSTANCE;
    }

    private boolean isEmptyType(FieldValue value){
        return value instanceof EmptyValue ||
                value instanceof ArrayValue ||
                value instanceof BinaryValue ||
                value instanceof BooleanValue ||
                value instanceof IntegerValue ||
                value instanceof JsonNullValue ||
                value instanceof LongValue ||
                value instanceof MapValue;
    }

    @Override
    public int compareTo(FieldValue other) {
        if (other instanceof NullValue) {
            return 0;
        }
        if (isEmptyType(other)){
            return 0;
        }
        return -1;
    }


    @Override
    public String getString() {
        return toJson(null);
    }

    @Override
    public String toJson(JsonOptions options) {
        return "null";
    }

    @Override
    public boolean equals(Object other) {
        return other == INSTANCE;
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    /**
     * @hidden
     */
    @Override
    public long sizeof() {
        return 0;
    }
}
