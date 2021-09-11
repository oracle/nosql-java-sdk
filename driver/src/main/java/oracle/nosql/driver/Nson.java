/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import java.io.IOException;
import java.math.BigDecimal;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Stack;

import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.BinaryValue;
import oracle.nosql.driver.values.BooleanValue;
import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.EmptyValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.FieldValueEventHandler;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonNullValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.driver.values.TimestampValue;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;
import oracle.nosql.driver.util.NettyByteOutputStream;
import oracle.nosql.driver.util.SerializationUtil;

/**
 * @hidden
 * This class implements the NSON format including serialization to the
 * format using a {@link FieldValueEventHandler} instance and deserialization
 * from the format by generating {@link FieldValueEventHandler} events from
 * the binary format.
 * <p>
 * The class also has methods to serialize and deserialize primitive values
 * according to the format. Serialization is done by using the
 * {@link NsonSerializer} class and passing it events. Deserialization is
 * performed by passing NSON and a {@link FieldValueEventHandler} to
 * {@link #generateEventsFromNson}
 * <p>
 * The NSON format
 *
 * The format is relatively simple:
 * &lt;byte&gt; field type
 * &lt;variable&gt; field value
 *

 * Primitive/atomic types have well-known formats and lengths. An NSON "row"
 * can be as simple as a single primitive. For example a single byte
 * representing the JSON null value is the smallest possible NSON document.
 * Complex types (map and array) include their total size in bytes, allowing
 * navigational code to skip them entirely if they are not of interest.
 * Details of the format of each data type is below.
 *
 * Data Types
 *
 * Array
 *  Tag value: 0
 *  Value:
 *    4-byte (unpacked int) – size of the entire array in bytes
 *    4-byte (unpacked int) – number of elements in the array
 *  The array fields, which are NSON values. Because NSON is schemaless the
 *  array elements may be of any type
 *
 * Binary
 *  Tag value: 1
 *  Value:
 *    size of array in bytes (can be 0)
 *    the bytes
 *
 * Boolean
 *  Tag value: 2
 *  Value (byte): 0 (false) or non-zero (true)
 *
 * Double
 *  Tag value: 3
 *  Value: double value as written by Java's DataOutput class (TBD: describe this
 *  in more detail)
 *
 * Integer
 *  Tag value: 4
 *  Value: packed int (see below)
 *
 * Long
 *  Tag value: 5
 *  Value: packed long (see below)
 *
 * Map
 *  Tag value: 6
 *  Value:
 *    4-byte (unpacked int) – size of the entire map in bytes (value as written
 *      by Java's DataOutput.writeInt)
 *    4-byte (unpacked int) – number of elements in the map (value as written
 *      by Java's DataOutput.writeInt)
 *    The map fields, which are:
 *      key – String (see String below), cannot be null
 *      value – an NSON value. Because NSON is schemaless the map values may be
 *              of any type
 *
 * String
 *  Tag value: 7
 *  Value: UTF-8 encoded string, as bytes
 *    The first byte is packed integer and is the length of the string. If
 *    if the string is null a length of -1 is used
 *    the rest of the bytes are the string encoded as utf-8
 *
 * Timestamp
 *  Tag value: 8
 *  Value: ISO 8601 formatted string (see String above)
 *
 * Number
 *  Tag value: 9
 *  Value: A string (see String above) representing a BigDecimal (Java) value.
 *         TBD: describe in detail
 *
 * Json Null
 *  Tag value: 10
 *  Value: none
 *
 * Null
 *  Tag value: 11
 *  Value: none
 *
 * Packed formats (TBD)
 *   packed int, long -- – describe algorithm
 *   unpacked int – endian?
 *   Java BigDecimal/number format
 */
public class Nson {

    /**
     * Data types
     */
    public static final int TYPE_ARRAY = 0;
    public static final int TYPE_BINARY = 1;
    public static final int TYPE_BOOLEAN = 2;
    public static final int TYPE_DOUBLE = 3;
    public static final int TYPE_INTEGER = 4;
    public static final int TYPE_LONG = 5;
    public static final int TYPE_MAP = 6;
    public static final int TYPE_STRING = 7;
    public static final int TYPE_TIMESTAMP = 8;
    public static final int TYPE_NUMBER = 9;
    public static final int TYPE_JSON_NULL = 10;
    public static final int TYPE_NULL = 11;
    public static final int TYPE_EMPTY = 12;

    /*
     * Serialization methods
     *  int - packed int
     *  long - packed long
     *  double - DataOutput double format
     *  timestamp - ISO 8601 string
     *  string - utf-8 encoded byte[]
     */

    /*
     * Primitives
     */
    static protected void writeInt(ByteOutputStream out,
                                   int value) throws IOException {
        SerializationUtil.writePackedInt(out, value);
    }

    static protected void writeLong(ByteOutputStream out,
                                    long value) throws IOException {
        SerializationUtil.writePackedLong(out, value);
    }

    static protected void writeDouble(ByteOutputStream out,
                                      double value) throws IOException {
        out.writeDouble(value);
    }

    static protected void writeTimestamp(ByteOutputStream out,
                                         TimestampValue value)
        throws IOException {
        /* TODO, temporary */
        writeString(out, value.getString());
    }

    static protected void writeString(ByteOutputStream out,
                                      String s) throws IOException {

        SerializationUtil.writeString(out, s);
    }

    /**
     * Writes a char array as a UTF8 byte array. This is used for
     * system queries that may contain a password.
     */
    static protected void writeCharArrayAsUTF8(ByteOutputStream out,
                                               char [] chars)
        throws IOException {
        ByteBuffer buf = StandardCharsets.UTF_8.encode(CharBuffer.wrap(chars));
        byte[] array = new byte[buf.limit()];
        buf.get(array);
        writeByteArray(out, array);
    }

    public static int getSerializedSize(FieldValue value) {

        try (ByteOutputStream out =
             NettyByteOutputStream.createNettyByteOutputStream()) {
            writeFieldValue(out, value);
            int ret = out.getOffset();
            return ret;
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Can't serialize field value: " + ioe.getMessage());
        }
    }

    /*
     * Serialize a generic FieldValue into the output stream
     */
    public static void writeFieldValue(ByteOutputStream out,
                                       FieldValue value)
        throws IOException {

        NsonSerializer ns = new NsonSerializer(out);
        FieldValueEventHandler.generate(value, ns);
    }

    public static void writeByteArray(ByteOutputStream out,
                                      byte[] array) throws IOException {
        SerializationUtil.writeByteArray(out, array);
    }

    /*
     * Writes a byte array with a full 4-byte int length
     */
    public static void writeByteArrayWithInt(ByteOutputStream out,
                                             byte[] array)
        throws IOException {
        out.writeInt(array.length);
        out.write(array);
    }

    /*
     * Deserialization
     */

    /*
     * Primitives
     */
    static protected int readInt(ByteInputStream in) throws IOException {
        return SerializationUtil.readPackedInt(in);
    }

    static protected long readLong(ByteInputStream in) throws IOException {
        return SerializationUtil.readPackedLong(in);
    }

    static protected double readDouble(ByteInputStream in) throws IOException {
        return in.readDouble();
    }

    static protected String readString(ByteInputStream in) throws IOException {
        return SerializationUtil.readString(in);
    }

    static protected byte[] readByteArray(ByteInputStream in)
        throws IOException {
        return SerializationUtil.readByteArray(in);
    }

    static protected byte[] readByteArray(ByteInputStream in, boolean skip)
        throws IOException {
        return SerializationUtil.readByteArray(in, skip);
    }

    static protected int[] readIntArray(ByteInputStream in)
        throws IOException {
        return SerializationUtil.readPackedIntArray(in);
    }

    /**
     * Reads a byte array that has a not-packed integer size
     */
    static protected byte[] readByteArrayWithInt(ByteInputStream in)
        throws IOException {
        int length = in.readInt();
        if (length <= 0) {
            throw new IOException(
                "Invalid length for prepared query: " + length);
        }
        byte[] query = new byte[length];
        in.readFully(query);
        return query;
    }

    /**
     * @hidden
     * An instance of FieldValueEventHandler that accepts events and adds them
     * to the protocol output stream.
     */
    public static class NsonSerializer implements FieldValueEventHandler {

        private final ByteOutputStream out;

        /*
         * Stack used to store offsets for maps and arrays for tracking
         * number of bytes used for a map or array.
         */
        private final Stack<Integer> offsetStack;
        /*
         * Stack used to store sizes for maps and arrays for tracking number of
         * elements in a map or array.
         */
        private final Stack<Integer> sizeStack;

        public NsonSerializer(ByteOutputStream out) {
            this.out = out;
            offsetStack = new Stack<Integer>();
            sizeStack = new Stack<Integer>();
        }

        /*
         * Maps and Arrays. These objects start with their total length,
         * allowing them to be optionally skipped on deserialization.
         *  1. start:
         *    make a 4-byte space for the ultimate length of the serialized
         *    object.
         *  2. save the offset on a stack
         *  3. start counting elements on a stack
         *  4. ... entries are written
         *  5. end:
         *    a. pop the offset stack to get the original length offset
         *    write the real length into the spot that was held
         *    b. pop the size stack to get the number of elements
         *    write the real number of elements the spot that was held
         * NOTE: a full 4-byte integer is used to avoid the variable-length
         * encoding used by compressed integers.
         *
         * It would be more efficient and avoid an extra stack with pop/push
         * for each map/array element to rely on the size from the caller
         * but counting elements here is safer and doesn't rely on the caller
         * having access to the size information. For example, a caller may be
         * turning a List (via iterator) into an array. That is less likely
         * for a Map but it's simplest to keep them the same. Alternatively
         * the caller could track number of elements and send it correctly in
         * the end* calls but again, that relies on the caller.
         */
        @Override
        public void startMap(int size) throws IOException {
            out.writeByte(TYPE_MAP);
            int lengthOffset = out.getOffset();
            out.writeInt(0); // size in bytes
            out.writeInt(0); // number of elements
            offsetStack.push(lengthOffset);
            sizeStack.push(0);
        }

        @Override
        public void startArray(int size) throws IOException {
            out.writeByte(TYPE_ARRAY);
            int lengthOffset = out.getOffset();
            out.writeInt(0); // size in bytes
            out.writeInt(0); // number of elements
            offsetStack.push(lengthOffset);
            sizeStack.push(0);
        }

        @Override
        public void endMap(int size) throws IOException {
            int lengthOffset = offsetStack.pop();
            int numElems = sizeStack.pop();
            int start = lengthOffset + 4;
            /*
             * write size in bytes, then number of elements into the space
             * reserved
             */
            out.writeIntAtOffset(lengthOffset, out.getOffset() - start);
            out.writeIntAtOffset(lengthOffset + 4, numElems);
        }

        @Override
        public void endArray(int size) throws IOException {
            int lengthOffset = offsetStack.pop();
            int numElems = sizeStack.pop();
            int start = lengthOffset + 4;
            /*
             * write size in bytes, then number of elements into the space
             * reserved
             */
            out.writeIntAtOffset(lengthOffset, out.getOffset() - start);
            out.writeIntAtOffset(lengthOffset + 4, numElems);
        }

        @Override
        public boolean startMapField(String key) throws IOException {
            writeString(out, key);
            return false; /* don't skip */
        }

        @Override
        public void endMapField(String key) throws IOException {
            /* add one to number of elements */
            incrSize();
        }

        @Override
        public void endArrayField(int index) throws IOException {
            /* add one to number of elements */
            incrSize();
        }

        @Override
        public void booleanValue(boolean value) throws IOException {
            out.writeByte(TYPE_BOOLEAN);
            out.writeBoolean(value);
        }

        @Override
        public void binaryValue(byte[] byteArray) throws IOException {
            out.writeByte(TYPE_BINARY);
            writeByteArray(out, byteArray);
        }

        @Override
        public void stringValue(String value) throws IOException {
            out.writeByte(TYPE_STRING);
            writeString(out, value);
        }

        @Override
        public void integerValue(int value) throws IOException {
            out.writeByte(TYPE_INTEGER);
            writeInt(out, value);
        }

        @Override
        public void longValue(long value) throws IOException {
            out.writeByte(TYPE_LONG);
            writeLong(out, value);
        }

        @Override
        public void doubleValue(double value) throws IOException {
            out.writeByte(TYPE_DOUBLE);
            writeDouble(out, value);
        }

        @Override
        public void numberValue(BigDecimal value) throws IOException {
            out.writeByte(TYPE_NUMBER);
            writeString(out, value.toString());
        }

        @Override
        public void timestampValue(TimestampValue timestamp)
            throws IOException {

            out.writeByte(TYPE_TIMESTAMP);
            writeTimestamp(out, timestamp);
        }

        @Override
        public void jsonNullValue() throws IOException {
            out.writeByte(TYPE_JSON_NULL);
        }

        @Override
        public void nullValue() throws IOException {
            out.writeByte(TYPE_NULL);
        }

        @Override
        public void emptyValue() throws IOException {
            out.writeByte(TYPE_EMPTY);
        }

        private void incrSize() {
            int value = sizeStack.pop();
            sizeStack.push(value + 1);
        }
    }

    /**
     * @hidden
     *
     * An instance of FieldValueEventHandler that accepts events and constructs
     * a {@link FieldValue} instance. This is used for creating instances
     * from the wire protocol.
     *
     * In order to handle creation of nested complex types such as maps and
     * arrays stacks are maintained.

     * The current FieldValue instance is available using the
     * getCurrentValue() method.
     *
     * This class is public only so it can be tested.
     */
    public static class FieldValueCreator implements FieldValueEventHandler {

        private Stack<MapValue> mapStack;
        private Stack<ArrayValue> arrayStack;

        /*
         * A stack of map keys is needed to handle the situation where maps
         * are nested.
         */
        private Stack<String> keyStack;
        private MapValue currentMap;
        private ArrayValue currentArray;
        private String currentKey;
        private FieldValue currentValue;

        private void pushMap(MapValue map) {
            if (currentMap != null) {
                if (mapStack == null) {
                    mapStack = new Stack<MapValue>();
                }
                mapStack.push(currentMap);
            }
            currentMap = map;
            currentValue = map;
        }

        private void pushArray(ArrayValue array) {
            if (currentArray != null) {
                if (arrayStack == null) {
                    arrayStack = new Stack<ArrayValue>();
                }
                arrayStack.push(currentArray);
            }
            currentArray = array;
            currentValue = array;
        }

        private void pushKey(String key) {
            if (currentKey != null) {
                if (keyStack == null) {
                    keyStack = new Stack<String>();
                }
                keyStack.push(currentKey);
            }
            currentKey = key;
        }

        /**
         * Returns the current FieldValue if available
         *
         * @return the current value
         */
        public FieldValue getCurrentValue() {
            return currentValue;
        }

        @Override
        public void startMap(int size) throws IOException {
            /* maintain insertion order */
            pushMap(new MapValue(true, size));
        }

        @Override
        public void startArray(int size) throws IOException {
            pushArray(new ArrayValue(size));
        }

        @Override
        public void endMap(int size) throws IOException {
            /*
             * The in-process map becomes the currentValue
             */
            currentValue = currentMap;
            if (mapStack != null && !mapStack.empty()) {
                currentMap = mapStack.pop();
            } else {
                currentMap = null;
            }
        }

        @Override
        public void endArray(int size) throws IOException {
            /*
             * The in-process array becomes the currentValue
             */
            currentValue = currentArray;
            if (arrayStack != null && !arrayStack.empty()) {
                currentArray = arrayStack.pop();
            } else {
                currentArray = null;
            }
        }

        @Override
        public boolean startMapField(String key) throws IOException {
            pushKey(key);
            return false; /* don't skip */
        }

        @Override
        public void endMapField(String key) throws IOException {
            /*
             * currentMap could be null if a subclass has suppressed
             * creation of a wrapper map for the entire FieldValue.
             * The "finder" code might do this
             */
            if (currentKey != null && currentMap != null) {
                currentMap.put(currentKey, currentValue);
            }
            if (keyStack != null && !keyStack.empty()) {
                currentKey = keyStack.pop();
            } else {
                currentKey = null;
            }
            // currentValue undefined right now...
        }

        @Override
        public void endArrayField(int index) throws IOException {
            if (currentArray != null) {
                currentArray.add(currentValue);
            }
        }

        @Override
        public void booleanValue(boolean value) throws IOException {
            currentValue = BooleanValue.getInstance(value);
        }

        @Override
        public void binaryValue(byte[] byteArray) throws IOException {
            currentValue = new BinaryValue(byteArray);
        }

        @Override
        public void stringValue(String value) throws IOException {
            currentValue = new StringValue(value);
        }

        @Override
        public void integerValue(int value) throws IOException {
            currentValue = new IntegerValue(value);
        }

        @Override
        public void longValue(long value) throws IOException {
            currentValue = new LongValue(value);
        }

        @Override
        public void doubleValue(double value) throws IOException {
            currentValue = new DoubleValue(value);
        }

        @Override
        public void numberValue(BigDecimal value) throws IOException {
            currentValue = new NumberValue(value);
        }

        @Override
        public void timestampValue(TimestampValue timestamp) {
            currentValue = timestamp;
        }

        @Override
        public void jsonNullValue() throws IOException {
            currentValue = JsonNullValue.getInstance();
        }

        @Override
        public void nullValue() throws IOException {
            currentValue = NullValue.getInstance();
        }

        @Override
        public void emptyValue() throws IOException {
            currentValue = EmptyValue.getInstance();
        }
    }

    /*
     * Read the protocol input stream and send events to a handler that
     * creates a FieldValue.
     */
    public static FieldValue readFieldValue(ByteInputStream in)
        throws IOException {
        FieldValueCreator handler = new FieldValueCreator();
        generateEventsFromNson(handler, in, false);
        /*
         * Results accumulated in the handler
         */
        return handler.getCurrentValue();
    }

    /**
     * Reads NSON from the input stream and generates events calling the
     * provided {@link FieldValueEventHandler} instance.
     *
     * @param handler the event handler
     * @param in the input stream that holds the NSON
     * @param skip true if the next field should be skipped
     * @throws IOException if there is a problem reading the input
     */
    public static void generateEventsFromNson(
        FieldValueEventHandler handler, ByteInputStream in, boolean skip)
        throws IOException {


        if (handler.stop()) {
            return;
        }
        int t = in.readByte();
        switch (t) {
        case TYPE_ARRAY:
            int length = in.readInt(); // length of serialized bytes
            if (skip) {
                in.skip(length);
            } else {
                int numElements = in.readInt();
                handler.startArray(numElements);
                if (handler.stop()) {
                    return;
                }
                for (int i = 0; i < numElements; i++) {
                    skip = handler.startArrayField(i);
                    if (handler.stop()) {
                        return;
                    }
                    generateEventsFromNson(handler, in, skip);
                    if (handler.stop()) {
                        return;
                    }
                    handler.endArrayField(i);
                    if (handler.stop()) {
                        return;
                    }
                }
                /* always call endArray, even if stopping */
                handler.endArray(numElements);
            }
            break;
        case TYPE_BINARY:
            byte[] byteVal = readByteArray(in, skip);
            if (!skip) {
                handler.binaryValue(byteVal);
            }
            break;
        case TYPE_BOOLEAN:
            boolean bval = in.readBoolean();
            if (!skip) {
                handler.booleanValue(bval);
            }
            break;
        case TYPE_DOUBLE:
            double dval = readDouble(in);
            if (!skip) {
                handler.doubleValue(dval);
            }
            break;
        case TYPE_INTEGER:
            int ival = readInt(in);
            if (!skip) {
                handler.integerValue(ival);
            }
            break;
        case TYPE_LONG:
            long lval = readLong(in);
            if (!skip) {
                handler.longValue(lval);
            }
            break;
        case TYPE_MAP:
            /* NOTE: length in bytes includes the 4 bytes for numelements */
            length = in.readInt(); // length of serialized bytes
            if (skip) {
                in.skip(length);
            } else {
                int numElements = in.readInt(); // size of map
                handler.startMap(numElements);
                if (handler.stop()) {
                    return;
                }
                for (int i = 0; i < numElements; i++) {
                    String key = readString(in);
                    boolean skipField = handler.startMapField(key);
                    if (handler.stop()) {
                        return;
                    }
                    generateEventsFromNson(handler, in, skipField);
                    if (handler.stop()) {
                        return;
                    }
                    handler.endMapField(key);
                    if (handler.stop()) {
                        return;
                    }
                }
                handler.endMap(numElements);
            }
            break;
        case TYPE_STRING:
            String sval = readString(in);
            if (!skip) {
                handler.stringValue(sval);
            }
            break;
        case TYPE_TIMESTAMP:
            String tval = readString(in);
            if (!skip) {
                handler.timestampValue(new TimestampValue(tval, 1));
            }
            break;
        case TYPE_NUMBER:
            String nval = readString(in);
            if (!skip) {
                handler.numberValue(new BigDecimal(nval));
            }
            break;
        case TYPE_JSON_NULL:
            if (!skip) {
                handler.jsonNullValue();
            }
            break;
        case TYPE_NULL:
            if (!skip) {
                handler.nullValue();
            }
            break;
        case TYPE_EMPTY :
            if (!skip) {
                handler.emptyValue();
            }
            break;
        default:
            throw new IllegalStateException("Unknown value type code: " + t);
        }
    }

    /*
     * Methods to read atomic types from the current position in an NSON
     * stream. If types do not match an exception is thrown.
     */

    /**
     * Reads an integer from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static int readNsonInt(ByteInputStream in) throws IOException {
        readType(in, TYPE_INTEGER);
        return readInt(in);
    }

    /**
     * Reads a long from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static long readNsonLong(ByteInputStream in) throws IOException {
        readType(in, TYPE_LONG);
        return readLong(in);
    }

    /**
     * Reads a double from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static double readNsonDouble(ByteInputStream in) throws IOException {
        readType(in, TYPE_DOUBLE);
        return readDouble(in);
    }

    /**
     * Reads a boolean from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static boolean readNsonBoolean(ByteInputStream in)
        throws IOException {

        readType(in, TYPE_BOOLEAN);
        return in.readBoolean();
    }

    /**
     * Reads a string from the {@link ByteInputStream}
     * @param in the input stream
     * @return the value
     * @throws IllegalArgumentException if the type at the stream is not
     * the one expected
     * @throws IOException if there are problems reading the stream
     */
    public static String readNsonString(ByteInputStream in)
        throws IOException {

        readType(in, TYPE_STRING);
        return readString(in);
    }

    private static void readType(ByteInputStream in, int expected)
        throws IOException {

        int type = in.readByte();
        if (type != expected) {
            throwTypeMismatch(expected, type);
        }
    }

    private static void throwTypeMismatch(int expected, int found) {
        throw new IllegalArgumentException(
            "Expected type not found, expected type: " + expected +
            ", found type: " + found);
    }
}
