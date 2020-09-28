/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.values;

import static oracle.nosql.driver.util.CheckNull.requireNonNull;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.math.BigDecimal;

import oracle.nosql.driver.JsonParseException;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonParser.Feature;
import com.fasterxml.jackson.core.JsonToken;

/**
 * @hidden
 * Internal use only
 *
 * A utility class for working with JSON and FieldValue instances.
 */
public class JsonUtils {

    protected static final JsonFactory factory = new JsonFactory();

    /*
     * Symbols used to converting hex string
     */
    private static final String HEX = "0123456789ABCDEF";

    public static FieldValue createValueFromJson(String jsonInput,
                                                 JsonOptions options) {
        requireNonNull(jsonInput,
                       "createValueFromJson: jsonInput must be non-null");

        JsonParser jp = null;
        try {
            jp = createParserWithOptions(jsonInput, options);
            return createValueFromJson(jp, true, options);
        } finally {
            if (jp != null) {
                try {
                    jp.close();
                } catch (IOException ioe) {
                    /* ignore */
                }
            }
        }
    }

    public static FieldValue createValueFromJson(InputStream jsonInput,
                                                 JsonOptions options) {

        requireNonNull(jsonInput,
                       "createValueFromJson: jsonInput must be non-null");
        JsonParser jp = null;
        try {
            jp = createParserWithOptions(jsonInput, options);
            return createValueFromJson(jp, true, options);
        } finally {
            if (jp != null) {
                try {
                    jp.close();
                } catch (IOException ioe) {
                    /* ignore */
                }
            }
        }
    }

    public static FieldValue createValueFromJson(Reader jsonInput,
                                                 JsonOptions options) {
        requireNonNull(jsonInput,
                       "createValueFromJson: jsonInput must be non-null");

        JsonParser jp = null;
        try {
            jp = createParserWithOptions(jsonInput, options);
            return createValueFromJson(jp, true, options);
        } finally {
            if (jp != null) {
                try {
                    jp.close();
                } catch (IOException ioe) {
                    /* ignore */
                }
            }
        }
    }

    static JsonParser createParserWithOptions(File file,
                                              JsonOptions options) {
        JsonParser jp =  null;
        try {
            jp = factory.createParser(file);
            addOptions(jp, options);
            return jp;
        } catch (IOException ioe) {
            throw createParseException(("Error parsing JSON: " +
                                        ioe.getMessage()), jp);
        }
    }

    static JsonParser createParserWithOptions(String jsonInput,
                                              JsonOptions options) {
        JsonParser jp =  null;
        try {
            jp = factory.createParser(jsonInput);
            addOptions(jp, options);
            return jp;
        } catch (IOException ioe) {
            throw createParseException(("Error parsing JSON: " +
                                        ioe.getMessage()), jp);
        }
    }

    static JsonParser createParserWithOptions(Reader jsonInput,
                                              JsonOptions options) {
        JsonParser jp =  null;
        try {
            jp = factory.createParser(jsonInput);
            addOptions(jp, options);
            return jp;
        } catch (IOException ioe) {
            throw createParseException(("Error parsing JSON: " +
                                        ioe.getMessage()), jp);
        }
    }

    static JsonParser createParserWithOptions(InputStream jsonInput,
                                              JsonOptions options) {
        JsonParser jp =  null;
        try {
            jp = factory.createParser(jsonInput);
            addOptions(jp, options);
            return jp;
        } catch (IOException ioe) {
            throw createParseException(("Error parsing JSON: " +
                                        ioe.getMessage()), jp);
        }
    }

    @SuppressWarnings("deprecation")
    private static void addOptions(JsonParser jp, JsonOptions options) {
        if (options != null) {
            if (options.getAllowComments()) {
                jp.enable(Feature.ALLOW_COMMENTS);
            }
            if (options.getAllowNonNumericNumbers()) {
                jp.enable(Feature.ALLOW_NON_NUMERIC_NUMBERS);
            }
            if (options.getAllowSingleQuotes()) {
                jp.enable(Feature.ALLOW_SINGLE_QUOTES);
            }
        }
    }


    /**
     * Construct a FieldValue based on arbitrary JSON from the incoming JSON
     * The top-level object may be any valid JSON:
     * 1. an object
     * 2. an array
     * 3. a scalar, including the JSON null value
     *
     * This code creates FieldValue types based on the type inferred from the
     * parser.
     */
    static FieldValue createValueFromJson(JsonParser jp,
                                          boolean getNext,
                                          JsonOptions options) {

        try {
            JsonToken token = (getNext ? jp.nextToken() : jp.getCurrentToken());
            if (token == null) {
                throw createParseException("Empty JSON", jp);
            }

            /*
             * TODO:
             *  o handle JsonOptions
             *  o include parsing location, etc in errors
             */
            switch (token) {
            case VALUE_STRING:
                return new StringValue(jp.getText());
            case VALUE_NUMBER_INT:
            case VALUE_NUMBER_FLOAT:

                /*
                 * If all numbers are to be forced to NumberValue do that here
                 */
                if (options != null && options.getNumericAsNumber()) {
                    return new NumberValue(jsonParserGetDecimalValue(jp));
                }

                /*
                 * Handle numeric types here. 4 types are supported:
                 *  INTEGER
                 *  LONG (long and integer)
                 *  DOUBLE (double and float)
                 *  NUMBER (anything that won't fit into the two above)
                 */
                JsonParser.NumberType numberType = jp.getNumberType();

                switch (numberType) {
                case BIG_INTEGER:
                case BIG_DECIMAL:
                    BigDecimal bd = jsonParserGetDecimalValue(jp);
                    return new NumberValue(bd);
                case INT:
                    return new IntegerValue(jp.getIntValue());
                case LONG:
                    return new LongValue(jp.getLongValue());
                case FLOAT:
                case DOUBLE:
                    double dbl = jp.getDoubleValue();
                    /*
                     * Jackson parse a floating-point numbers to a double:
                     *  - if the Math.abs(value) > Double.MAX_VALUE, return
                     *    Infinity.
                     *  - if the abs(value) is smaller than Double.MIN_VALUE,
                     *    then return Zero.
                     *
                     * So check if the double value is Infinity or Zero, try to
                     * read it as BigDecimal value. The real 0.0 is a special
                     * value, it is treated as valid double value.
                     */
                    if (Double.isInfinite(dbl) || dbl == 0.0) {
                        try {
                            /*
                             * If it's a string "Infinity", the parser will
                             * throw exception
                             */
                            bd = jsonParserGetDecimalValue(jp);
                            if (bd.compareTo(BigDecimal.ZERO) != 0) {
                            	return new NumberValue(bd);
                            }
                        } catch (JsonParseException e) {}
                    }
                    return new DoubleValue(dbl);
                }
                throw createParseException("Unexpected numeric type: " +
                                           numberType, jp);
            case VALUE_TRUE:
                return BooleanValue.trueInstance();
            case VALUE_FALSE:
                return BooleanValue.falseInstance();
            case VALUE_NULL:
                return JsonNullValue.getInstance();
            case START_OBJECT:
                return parseObject(jp, options);
            case START_ARRAY:
                return parseArray(jp, options);
            case FIELD_NAME:
            case END_OBJECT:
            case END_ARRAY:
            default:
                throw createParseException(
                    "Unexpected token while parsing JSON: " + token, jp);
            }
        } catch (IOException ioe) {
            throw createParseException(
                "Failed to parse JSON input: " + ioe.getMessage(), jp);
        }
    }

    /**
     * Creates a JSON map from the parsed JSON object.
     */
    private static FieldValue parseObject(JsonParser jp, JsonOptions options)
        throws IOException {

        MapValue map = null;
        if (options != null) {
            // 16 is the default capacity for a LinkedHashMap
            map = new MapValue(options.getMaintainInsertionOrder(), 16);
        } else {
            map = new MapValue();
        }

        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_OBJECT) {
            String fieldName = jp.getCurrentName();
            if (token == null || fieldName == null) {
                throw createParseException(
                    "null token or field name parsing JSON object", jp);
            }

            /* true tells the method to fetch the next token */
            FieldValue field = createValueFromJson(jp, true, options);
            map.put(fieldName, field);
        }
        return map;
    }

    /**
     * Creates a JSON array from the parsed JSON array by adding
     */
    private static FieldValue parseArray(JsonParser jp, JsonOptions options)
        throws IOException {

        ArrayValue array = new ArrayValue();

        JsonToken token;
        while ((token = jp.nextToken()) != JsonToken.END_ARRAY) {
            if (token == null) {
                throw createParseException(
                    "null token while parsing JSON array", jp);
            }

            /* false means don't get the next token, it's been fetched */
            array.add(createValueFromJson(jp, false, options));
        }
        return array;
    }

    public static BigDecimal jsonParserGetDecimalValue(JsonParser parser)
        throws IOException {

        assert(parser != null) : "The JsonParser should not be null";

        try {
            return parser.getDecimalValue();
        } catch (NumberFormatException nfe) {
            throw createParseException("Malformed numeric value: '" +
                                       parser.getText() + ": " + nfe.getMessage(),
                                       parser);
        }
    }

    private static JsonParseException createParseException(String msg,
                                                           JsonParser jp) {
        return new JsonParseException(msg,
                                      (jp == null ? null :
                                       jp.getCurrentLocation()));
    }

    /**
     * Convert a hex string to byte array
     * @param hexString the string
     * @return the bytes
     */
    public static byte[] convertHexToBytes(String hexString) {

        if (hexString.length() % 2 != 0) {
            throw new IllegalArgumentException("Invalid hex string length");
        }

        final byte[] result = new byte[hexString.length()/2];

        final int n = hexString.length();

        for (int i = 0; i < n; i += 2) {
            /* high bits */
            final int hb = HEX.indexOf(hexString.charAt(i));
            /* low bits */
            final int lb = HEX.indexOf(hexString.charAt(i + 1));
            result[i/2] = (byte)((hb << 4 ) | lb);
        }
        return result;
    }

    /**
     * Convert a byte array to hex string
     * @param byteArray the bytes
     * @return the string
     */
    public static String convertBytesToHex(byte[] byteArray) {

        final char[] hexValue = new char[byteArray.length * 2];

        final char[] hexSymbols = HEX.toCharArray();

        for (int i = 0; i < byteArray.length; i++) {
            final int current = byteArray[i] & 0xff;
            /* determine the Hex symbol for the last 4 bits */
            hexValue[i*2 + 1] = hexSymbols[current & 0x0f];
            /* determine the Hex symbol for the first 4 bits */
            hexValue[i*2] = hexSymbols[current >> 4];
        }
        return new String(hexValue);
    }
}
