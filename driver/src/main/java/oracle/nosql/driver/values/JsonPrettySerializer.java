/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.values;

import com.fasterxml.jackson.core.io.CharTypes;

/**
 * @hidden
 *
 * JsonPrettySerializer is a FieldValueEventHandler instance that creates a
 * JSON string value, pretty-printed. This class extends
 * {@link JsonSerializer} overriding a few methods for handling pretty
 * printing. The amount of indentation used for objects can be configured
 * by using the constructor that accepts an indentation argument,
 * {@link #JsonPrettySerializer(int, JsonOptions)}.
 */
public class JsonPrettySerializer extends JsonSerializer {


    /*
     * Todo: configurable?
     */
    protected static final int DEFAULT_INCR = 2;
    protected static String CR = "\n";
    protected static String SP = " ";

    protected final int incr;
    protected int currentIndent = 0;
    protected String indent;

    /**
     * Creates a new JsonPrettySerializer using default indentation.
     *
     * @param options {@link JsonOptions} to use for the serialization or null
     * for default behavior.
     */
    public JsonPrettySerializer(JsonOptions options) {
        super(options);
        KEY_SEP = " : ";
        incr = DEFAULT_INCR;
    }

    /**
     * Creates a new JsonPrettySerializer using the object indentation
     * provided.
     *
     * @param indentation the number of spaces to use for indentation of
     * the fields for each new object.
     *
     * @param options {@link JsonOptions} to use for the serialization or null
     * for default behavior.
     */
    public JsonPrettySerializer(int indentation, JsonOptions options) {
        super(options);
        if (indentation <= 0) {
            throw new IllegalArgumentException(
                "Indentation must be greater than 0");
        }
        incr = indentation;
        KEY_SEP = " : ";
    }

    /**
     * Creates an indentation string for objects.
     *
     * TBD, make more efficient -- maybe use the same StringBuilder and
     * change sizes.
     */
    private void changeIndent(int num) {
        currentIndent += num;
        StringBuilder isb = new StringBuilder();
        for (int i = 0; i < currentIndent; i++) {
            isb.append(" ");
        }
        indent = isb.toString();
    }

    @Override
    public void startMap(int size) {
        sb.append(START_OBJECT);
        changeIndent(incr);
    }

    @Override
    public void endMap(int size) {
        if (size > 0) {
            /* map entries have trailing "," */
            sb.setLength(sb.length() - 1);
        }
        changeIndent(-incr);
        sb.append(CR).append(indent).append(END_OBJECT);
    }

    @Override
    public void endArray(int size) {
        if (size > 0) {
            /* array elements have trailing ", " */
            sb.setLength(sb.length() - 2);
        }
        sb.append(END_ARRAY);
    }

    /*
     * A new map field adds a new line, indentation, a quoted
     * key with space before and after the key separator.
     */
    @Override
    public boolean startMapField(String key) {
        sb.append(CR).append(indent).append(QUOTE);
        CharTypes.appendQuoted(sb, key);
        sb.append(QUOTE).append(KEY_SEP);
        return false;
    }

    @Override
    public void endArrayField(int index) {
        sb.append(FIELD_SEP);
        /* add a space after the comma */
        sb.append(SP);
    }
}
