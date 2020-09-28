/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.values;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Map;

/**
 * FieldValueEventHandler is an event-driven interface that allows multiple
 * implementations of serializers and deserializers for a {@link FieldValue}.
 * The events correspond to the data model exposed by {@link FieldValue}.
 * <p>
 * Events can be generated from {@link FieldValue} instances themselves,
 * from a wire protocol, or from a generic program. Examples of uses include
 * <ul>
 * <li>Binary serializer for the wire protocol, capturing events generated
 * from a {@link FieldValue} instances</li>
 * <li>Binary de-serializer for the wire protocol, capturing events generated
 * from code that is reading the wire protocol</li>
 * <li>JSON serializer for {@link FieldValue} instances</li>
 * <li>JSON pretty-printfor {@link FieldValue} instances</li>
 * <li>Direct-from/to-object serializer or deserializer that operates on
 * POJOs intead of {@link FieldValue}</li>
 * </ul>
 *
 * In addition to the interface this file includes static methods to generate
 * events from a {@link FieldValue} instance. See {@link #generate}.
 * <p>
 * Example usage
 * <pre>
 *   MapValue map = new MapValue();
 *   map.put(...);
 *   ...
 *   JsonSerializer js = new JsonSerializer(null);
 *   FieldValueEventHandler.generate(map, js);
 *   String json = js.toString();
 * </pre>
 * <p>
 * Functionally here is how the event handler is to be used. Look at the
 * {@link #generate} method for implementation details. There are no
 * specific start/end events, although they could be added if deemed useful.
 * <p>
 * Simple (atomic) field value events are just that, e.g.
 * <pre>
 *   booleanValue(true);
 *   integerValue(6);
 * </pre>
 * Map values have <em>startMap</em> and <em>endMap</em> events that surround
 * them. These methods have a size argument to let the event handler know the
 * size of the map. It's possible that the size is not known but it's best to
 * provide one if possible, and in some cases (protocol serialization) it's
 * required. Map entries require key and value, where the value may be a nested
 * map or array. To account for nesting the events are:
 * <pre>
 *   startMap(1)
 *   startMapField(key) // pass the key String
 *   // an event that creates a value, including a nested map or array
 *   stringValue("a string")
 *   endMapField()
 *   endMap(1)
 * </pre>
 * Array values have <em>startArray</em> and <em>endArray</em> events that
 * surround them. These methods have a size argument to let the event handler
 * know the size of the array. It's possible that the size is not known but
 * it's best to provide one if possible, and in some cases (protocol
 * serialization) it's required. Array entry events are followed with
 * endArrayField events. This simplifies implementations that need to know if
 * they are working within an array. Here's an array example.
 * <pre>
 *   startArray(2)
 *   startMap(msize) // nested map
 *   endMap(msize)   // map is empty
 *   endArrayField()
 *   stringValue("a string") // add string to the array
 *   endArrayField()
 *   endArray(2)
 * </pre>
 */
public interface FieldValueEventHandler {

    /**
     * Start a MapValue. This method accepts the size of the map, but there
     * are situations where the size may not be known. In this case -1
     * should be passed and it is up to the implementing class to decide
     * if it can operate without a size and if not, throw
     * IllegalArgumentException.
     *
     * @param size the number of entries in the map or -1 if not known.
     * @throws IllegalArgumentException if the size is invalid or cannot be
     * handled by the implementation.
     * @throws IOException conditionally, based on implementation
     */
    void startMap(int size) throws IOException;

    /**
     * Start an ArrayValue. This method accepts the size of the array, but there
     * are situations where the size may not be known. In this case -1
     * should be passed and it is up to the implementing class to decide
     * if it can operate without a size and if not, throw
     * IllegalArgumentException.
     *
     * @param size the number of entries in the array or -1 if not known.
     * @throws IllegalArgumentException if the size is invalid or cannot be
     * handled by the implementation.
     * @throws IOException conditionally, based on implementation
     */
    void startArray(int size) throws IOException;

    /**
     * End a MapValue.
     *
     * @param size the number of entries in the map or -1 if not known.
     * @throws IOException conditionally, based on implementation
     */
    void endMap(int size) throws IOException;

    /**
     * End an ArrayValue.
     *
     * @param size the number of entries in the array or -1 if not known.
     * @throws IOException conditionally, based on implementation
     */
    void endArray(int size) throws IOException;

    /**
     * Start a field in a map.
     *
     * @param key the key of the field.
     * @throws IOException conditionally, based on implementation
     */
    void startMapField(String key) throws IOException;

    /**
     * End a field in a map.
     * @throws IOException conditionally, based on implementation
     */
    void endMapField() throws IOException;

    /**
     * End a field in an array. There is no corresponding start for
     * array entries. This allows, for example, JSON serializers to insert
     * array value separators without the complexity of tracking the
     * entire event sequence.
     * @throws IOException conditionally, based on implementation
     */
    void endArrayField() throws IOException;

    /**
     * A boolean value
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    void booleanValue(boolean value) throws IOException;

    /**
     * A binary value
     *
     * @param byteArray the byte[] value
     * @throws IOException conditionally, based on implementation
     */
    void binaryValue(byte[] byteArray) throws IOException;

    /**
     * A String value
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    void stringValue(String value) throws IOException;

    /**
     * An integer value
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    void integerValue(int value) throws IOException;

    /**
     * A long value
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    void longValue(long value) throws IOException;

    /**
     * A double value
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    void doubleValue(double value) throws IOException;

    /**
     * A Number value.
     *
     * @param value the value
     * @throws IOException conditionally, based on implementation
     */
    void numberValue(BigDecimal value) throws IOException;

    /**
     * A Timestamp value
     *
     * @param timestamp the value
     * @throws IOException conditionally, based on implementation
     */
    void timestampValue(TimestampValue timestamp) throws IOException;

    /**
     * A JsonNullValue
     * @throws IOException conditionally, based on implementation
     */
    void jsonNullValue() throws IOException;

    /**
     * A NullValue
     * @throws IOException conditionally, based on implementation
     */
    void nullValue() throws IOException;

    /**
     * An EmptyValue
     * @throws IOException conditionally, based on implementation
     */
    void emptyValue() throws IOException;

    /**
     * Generates events from a {@link FieldValue} instance sending them to
     * the {@link FieldValueEventHandler} provided.
     *
     * @param value the FieldValue used to generate events
     * @param handler the handler to use
     * @throws IOException conditionally, based on implementation
     */
    public static void generate(FieldValue value,
                                FieldValueEventHandler handler)
        throws IOException {

        FieldValue.Type type = value.getType();
        switch (type) {
            case ARRAY:
                generateForArray(value.asArray(), handler);
                break;
            case BINARY:
                handler.binaryValue(value.getBinary());
                break;
            case BOOLEAN:
                handler.booleanValue(value.getBoolean());
                break;
            case DOUBLE:
                handler.doubleValue(value.getDouble());
                break;
            case INTEGER:
                handler.integerValue(value.getInt());
                break;
            case LONG:
                handler.longValue(value.getLong());
                break;
            case MAP:
                generateForMap(value.asMap(), handler);
                break;
            case STRING:
                handler.stringValue(value.asString().getString());
                break;
            case TIMESTAMP:
                handler.timestampValue(value.asTimestamp());
                break;
            case NUMBER:
                handler.numberValue(value.asNumber().getValue());
                break;
            case JSON_NULL:
                handler.jsonNullValue();
                break;
            case NULL:
                handler.nullValue();
                break;
            case EMPTY:
                handler.emptyValue();
                break;
            default:
                throw new IllegalStateException(
                    "FieldValueEventHandler, unknown type " + type);
        }
    }

    /**
     * Generates events for {@link MapValue} sending them to the specified
     * {@link FieldValueEventHandler}.
     *
     * @param map the MapValue to use
     * @param handler the handler to use
     * @throws IOException conditionally, based on implementation
     */
    public static void generateForMap(MapValue map,
                                      FieldValueEventHandler handler)
        throws IOException {

        handler.startMap(map.size());
        for (Map.Entry<String, FieldValue> entry : map.entrySet()) {
            handler.startMapField(entry.getKey());
            generate(entry.getValue(), handler);
            handler.endMapField();
        }
        handler.endMap(map.size());
    }

    /**
     * Generates events for {@link ArrayValue} sending them to the specified
     * {@link FieldValueEventHandler}.
     *
     * @param array the ArrayValue to use
     * @param handler the handler to use
     * @throws IOException conditionally, based on implementation
     */
    public static void generateForArray(ArrayValue array,
                                        FieldValueEventHandler handler)
        throws IOException {

        handler.startArray(array.size());
        for (FieldValue value : array) {
            generate(value, handler);
            handler.endArrayField();
        }
        handler.endArray(array.size());
    }
}
