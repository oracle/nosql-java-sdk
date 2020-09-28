/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static oracle.nosql.driver.util.TimestampUtil.formatString;
import static oracle.nosql.driver.util.TimestampUtil.parseString;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.Arrays;
import java.util.Iterator;
import java.util.TimeZone;

import oracle.nosql.driver.ops.serde.BinaryProtocol;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.BinaryValue;
import oracle.nosql.driver.values.BooleanValue;
import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.EmptyValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.FieldValue.Type;
import oracle.nosql.driver.values.FieldValueEventHandler;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.JsonUtils;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.driver.values.TimestampValue;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.ByteOutputStream;

import org.junit.Test;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;

/**
 * A set of test cases to exercise the FieldValue interfaces.
 */
public class ValueTest extends DriverTestBase {

    /**
     * This tests that there's a version string available. It's not
     * related to Value instances; it just needed a home.
     */
    @Test
    public void testVersionString() {
        final String version = System.getProperty("driverversion");
        assertEquals(version, NoSQLHandleConfig.getLibraryVersion());
    }

    /**
     * Test "Infinity"
     */
    @Test
    public void testInfinityStr() {
        String str = "{\"num\":1.7976931348623157E+308,"
                    + "\"col_double\":1.7976931348623157E308,"
                    + "\"col_float\":Infinity}";
        JsonOptions options =
            new JsonOptions().setAllowNonNumericNumbers(true);
        MapValue mv = (MapValue)JsonUtils.createValueFromJson(str, options);
        assertTrue(mv.toString().contains("Infinity"));
    }

    /**
     * Test "+Infinity"
     */
    @Test
    public void testPositiveInfinityStr() {
        String str = "{\"num\":1.7976931348623157E+308,"
                    + "\"col_double\":1.7976931348623157E308,"
                    + "\"col_float\":+Infinity}";
        JsonOptions options =
            new JsonOptions().setAllowNonNumericNumbers(true);
        MapValue mv = (MapValue)JsonUtils.createValueFromJson(str, options);
        assertTrue(mv.toString().contains("Infinity"));
    }

    /**
     * Test "-Infinity"
     */
    @Test
    public void testNegativeInfinityStr() {
        String str = "{\"num\":1.7976931348623157E+308,"
                    + "\"col_double\":1.7976931348623157E308,"
                    + "\"col_float\":-Infinity}";
        JsonOptions options =
            new JsonOptions().setAllowNonNumericNumbers(true);
        MapValue mv = (MapValue)JsonUtils.createValueFromJson(str, options);
        assertTrue(mv.toString().contains("-Infinity"));
    }

    /**
     * Test "NaN
     */
    @Test
    public void testNaNStr() {
        String str = "{\"num\":1.7976931348623157E+308,"
                    + "\"col_double\":1.7976931348623157E308,"
                    + "\"col_float\":NaN}";
        JsonOptions options =
            new JsonOptions().setAllowNonNumericNumbers(true);
        MapValue mv = (MapValue)JsonUtils.createValueFromJson(str, options);
        assertTrue(mv.toString().contains("NaN"));
    }

    /**
     * Test "nan"
     */
    @Test
    public void testnanStr() {
        String str = "{\"num\":1.7976931348623157E+308,"
                    + "\"col_double\":1.7976931348623157E308,"
                    + "\"col_float\":nan}";
        JsonOptions options =
            new JsonOptions().setAllowNonNumericNumbers(true);
        expectParseException(str, options);
    }

    /**
     * Test "infinity"
     */
    @Test
    public void testinfinityStr() {
        String str = "{\"num\":1.7976931348623157E+308,"
                    + "\"col_double\":1.7976931348623157E308,"
                    + "\"col_float\":infinity}";
        JsonOptions options =
            new JsonOptions().setAllowNonNumericNumbers(true);
        expectParseException(str, options);
    }

    /**
     * Test "INF"
     */
    @Test
    public void testINFStr() {
        String str = "{\"num\":1.7976931348623157E+308,"
                    + "\"col_double\":1.7976931348623157E308,"
                    + "\"col_float\":INF}";
        JsonOptions options =
            new JsonOptions().setAllowNonNumericNumbers(true);
        expectParseException(str, options);
    }

    /**
     * Test random_string "asbq"
     */
    @Test
    public void testRandomStr() {
        String str = "{\"num\":1.7976931348623157E+308,"
                    + "\"col_double\":1.7976931348623157E308,"
                    + "\"col_float\":asbq}";
        JsonOptions options =
            new JsonOptions().setAllowNonNumericNumbers(true);
        expectParseException(str, options);
    }

    /**
     * Tests FieldValue.createFromJson using very simple JSON string input.
     *
     * TODO:
     *  - pretty print
     */
    @Test
    public void testCreateFromJson() {
        final String intDoc = "123";
        final String longDoc = "123456789012";
        final String doubleDoc = "1.234";
        final String nanDoc = "NaN";
        final String numberDoc = "12345678901234567890";
        final String stringDoc = "\"abc\"";
        final String booleanDoc = "false";
        final String nullDoc = "null";
        final String mapDoc = "{}";
        final String arrayDoc = "[1, 2, \"5\"]";
        final String singleQuoteDoc = "{'a': 'b'}";
        final String commentDoc =
            "{/* comment a */ \"a\": 5 } //another comment";

        JsonOptions options = new JsonOptions();

        assertTypeFromJson(intDoc, FieldValue.Type.INTEGER, options);
        assertTypeFromJson(longDoc, FieldValue.Type.LONG, options);
        assertTypeFromJson(doubleDoc, FieldValue.Type.DOUBLE, options);
        assertTypeFromJson(numberDoc, FieldValue.Type.NUMBER, options);
        assertTypeFromJson(stringDoc, FieldValue.Type.STRING, options);
        assertTypeFromJson(booleanDoc, FieldValue.Type.BOOLEAN, options);
        assertTypeFromJson(nullDoc, FieldValue.Type.JSON_NULL, options);
        assertTypeFromJson(mapDoc, FieldValue.Type.MAP, options);
        assertTypeFromJson(arrayDoc, FieldValue.Type.ARRAY, options);

        /*
         * Test some options
         */

        /*
         * Numeric values as NumberValue (BigDecimal).
         */
        options.setNumericAsNumber(true);
        assertTypeFromJson(intDoc, FieldValue.Type.NUMBER, options);
        assertTypeFromJson(longDoc, FieldValue.Type.NUMBER, options);
        assertTypeFromJson(doubleDoc, FieldValue.Type.NUMBER, options);
        assertTypeFromJson(numberDoc, FieldValue.Type.NUMBER, options);
        options.setNumericAsNumber(false);

        /*
         * non-numerics and DOUBLE
         *
         * NOTE: Jackson doc implies that it should accept "INF" and "-INF" as
         * well as "NaN" but those seem to fail, at least standalone.
         */
        options.setAllowNonNumericNumbers(true);
        assertTypeFromJson(nanDoc, FieldValue.Type.DOUBLE, options);
        FieldValue dval = FieldValue.createFromJson(nanDoc, options);
        assertEquals(Double.NaN, dval.getDouble(), 0);
        options.setAllowNonNumericNumbers(false);


        /* single quotes */
        options.setAllowSingleQuotes(true);
        assertTypeFromJson(singleQuoteDoc, FieldValue.Type.MAP, options);
        options.setAllowSingleQuotes(false);

        /* comments */
        options.setAllowComments(true);
        assertTypeFromJson(commentDoc, FieldValue.Type.MAP, options);
        options.setAllowComments(false);

        /*
         * Some error paths, based on options
         */
        expectParseError(singleQuoteDoc, options);
        expectParseError(nanDoc, options);
        expectParseError(commentDoc, options);
    }

    /**
     * Tests the use of line and column in JsonParseException
     */
    @Test
    public void testParseException() {
        final String emptyDoc = "";
        final String line1Doc = "{\"a\": \"b'}";
        final String line2Doc = "{\"a\":\n\"b'}";

        JsonOptions options = new JsonOptions();
        expectParseError(emptyDoc, options, 1, 1);
        expectParseError(line1Doc, options, 1, 8);
        expectParseError(line2Doc, options, 2, 2);
    }

    @Test
    public void testArrayIterable() {
        final String[] ss = {"a", "b"};
        final String[] ss1 = {"c", "d"};
        final int[] ii = {1,2,3,4};

        ArrayValue array = new ArrayValue();
        array.add(1).add(2).add(3);

        int count = 0;
        for (@SuppressWarnings("unused") FieldValue val : array) {
            ++count;
        }
        assertEquals(count, array.size());

        Iterator<FieldValue> it = array.iterator();
        while (it.hasNext()) {
            it.next();
            it.remove();
        }
        assertEquals(0, array.size());

        roundTrip(array);

        /*
         * addAll using Iterator
         */
        array.addAll(Arrays.stream(ss).map(s -> new StringValue(s)).iterator());
        it = array.iterator();
        assertTrue(array.size() == 2);

        /*
         * addAll at a specific index using Iterator
         */
        array.addAll(1, Arrays.stream(ss1).map(s -> new StringValue(s)).iterator());
        assertTrue(array.size() == 4);
        assertTrue(array.get(2).getString().equals("d"));
        roundTrip(array);

        /*
         * addAll using Stream and int array
         */
        array.addAll(Arrays.stream(ii).mapToObj(IntegerValue::new));
        assertTrue(array.size() == 8);
        assertTrue(array.get(4).getInt() == 1);

        /*
         * addAll using Stream at a specific index
         */
        array.addAll(3, Arrays.stream(ii).mapToObj(IntegerValue::new));
        assertTrue(array.size() == 12);
        assertTrue(array.get(3).getInt() == 1);
    }


    @Test
    public void testMap() {
        MapValue map1 = new MapValue();
        MapValue map2 = new MapValue();
        map1.put("a", 1).put("d", "xyz");

        /*
         * Test Stream interface
         */
        map2.addAll(map1.entrySet().stream());
        assertEquals(map1, map2);

        /* Test compareTo() method */
        /*
         * map1: {"a":1,"d":"xyz"}
         * map2: {"a":1,"d":"xyz"}
         */
        assertTrue(map1.compareTo(map2) == 0);

        /*
         * map1: {"a":1,"d":"xyz"}
         * map2: {"d":"xyz"}
         */
        map2.remove("a");
        assertTrue(map1.compareTo(map2) > 0);

        /*
         * map1: {"a":1,"d":"xyz"}
         * map2: {"A":2, "d":"xyz"}
         */
        map2.put("A", 2);
        assertTrue(map1.compareTo(map2) > 0);

        /*
         * map1: {"a":1,"d":"xyz"}
         * map2: {"A":2, "a":0, "d":"xyz"}
         */
        map2.put("a", 0);
        assertTrue(map1.compareTo(map2) > 0);

        /*
         * map1: {"a":1,"d":"xyz"}
         * map2: {"A":2, "a":2, "d":"xyz"}
         */
        map2.put("a", 2);
        assertTrue(map1.compareTo(map2) < 0);

        /*
         * map1: {"a":1,"d":"xyz"}
         * map2: {"A":2, "a":1, "d":"xyz"}
         */
        map2.put("a", 1);
        assertTrue(map1.compareTo(map2) < 0);

        /*
         * map1: {"a":1,"d":"xyz"}
         * map2: {"A":2, "a":"abc", "d":"xyz"}
         */
        map2.put("a", "abc");
        try {
            map1.compareTo(map2);
            fail("Expect to catch ClassCastException but not");
        } catch (ClassCastException cce) {
        }
    }

    @SuppressWarnings("unused")
    @Test
    public void testEmpty() {

        /*
         * TODO... test use, toJson, toString, etc
         */
        EmptyValue val = EmptyValue.getInstance();
    }

    @Test
    public void testTimestamp() {
        String[] texts = new String[] {
            "1970-01-01",
            "2017-07-15T15",
            "2017-07-15T15:18",
            "2017-07-15T15:18:59",
            "2017-07-15T15:18:59.123456789",
            "-1017-12-01T10:11:01.987",
            "-6383-01-01T00:00:00",
            "9999-12-31T23:59:59.999999999",
        };

        for (String s : texts) {
            Timestamp ts = parseString(s);
            TimestampValue tsv1 = new TimestampValue(s);
            TimestampValue tsv2 = new TimestampValue(ts);
            TimestampValue tsv3 = new TimestampValue(ts.getTime());

            assertTrue("Wrong tsv1",
                tsv1.getTimestamp().compareTo(ts) == 0);
            assertTrue("Wrong tsv2",
                tsv2.getTimestamp().compareTo(ts) == 0);
            assertTrue("Wrong tsv3",
                tsv3.getTimestamp().compareTo(new Timestamp(ts.getTime())) == 0);
            assertTrue("Wrong tsv1", tsv1.getLong() == ts.getTime());

            assertTrue("Compare tsv1 with tsv2 failed: " + tsv1 + " vs " + tsv2,
                tsv1.equals(tsv2));
            if (ts.getNanos() % 1000000 == 0) {
                assertTrue("Compare tsv1 with tsv3 failed: " + tsv1 + " vs " +
                    tsv3, tsv1.equals(tsv3));
            } else {
                assertTrue("Compare tsv1 with tsv3 failed: " + tsv1 + " vs " +
                    tsv3, tsv1.compareTo(tsv3) > 0);
            }

            assertTrue("Wrong string for tsv1: ",
                tsv1.getString().indexOf(s) == 0);
            assertTrue("tsv2 long value should equals to that of tvs3",
                tsv2.getLong() == tsv3.getLong());

            roundTrip(tsv1);
            roundTrip(tsv2);
            roundTrip(tsv3);
        }

        /* Test invalid ISO8601 format */
        String text;
        TimestampValue tsv;
        try {
            text = "Jan 1, 1970";
            tsv = new TimestampValue(text);
            fail("The timestamp string is not in IS8601 format, should fail");
        } catch (IllegalArgumentException iae) {
        }

        /* Test toJson(JsonOptions) */
        text = "2017-07-15T15:18:59.123456789";
        tsv = new TimestampValue(text);

        JsonOptions options = new JsonOptions().setTimestampAsString(true);
        assertTrue(tsv.toJson().equals("\"" + text + "\""));
        assertTrue(tsv.toJson(options).equals("\"" + text + "\""));

        options = new JsonOptions().setTimestampAsLong(true);
        assertTrue(tsv.toJson(options).equals(String.valueOf(tsv.getLong())));

        /* Test MapValue */
        text = "2017-07-15T15:18:59.556";
        Timestamp tsTest = parseString(text);

        MapValue value = new MapValue();
        value.put("string", "Json Wu");
        value.put("long", 100L);
        value.put("dateTimestamp", tsTest);
        value.put("dateLong", tsTest.getTime());
        value.put("dateString", text);
        String json = value.toJson();

        roundTrip(value);

        /* Creates MapValue from the JSON */
        MapValue value1 = (MapValue)MapValue.createFromJson(json, null);
        assertTrue("Wrong type of dataTimestamp",
            value1.getType("dateTimestamp") == Type.STRING);

        Timestamp ts1 = value1.getTimestamp("dateTimestamp");
        assertTrue("Wrong Timestamp value of dateTimestamp", ts1.equals(tsTest));

        ts1 = value1.getTimestamp("dateLong");
        assertTrue("Wrong Timestamp value of dateLong", ts1.equals(tsTest));

        ts1 = value1.getTimestamp("dateString");
        assertTrue("Wrong Timestamp value of dateLong", ts1.equals(tsTest));

        ts1 = value1.getTimestamp("long");
        assertTrue("Wrong value of long", ts1.getTime() == 100);

        try {
            value1.getTimestamp("string");
        } catch (IllegalArgumentException iae) {
        }

        /* JsonOptions.setTimestampAsString(true) */
        options = new JsonOptions().setTimestampAsString(true);
        json = value.toJson(options);

        value1 = (MapValue)MapValue.createFromJson(json, options);
        assertTrue("Wrong type of dateTimestamp",
            value1.getType("dateTimestamp") == Type.STRING);

        ts1 = value1.getTimestamp("dateTimestamp");
        assertTrue("Wrong Timestamp value of dateTimestamp", ts1.equals(tsTest));

        ts1 = value1.getTimestamp("dateLong");
        assertTrue("Wrong Timestamp value of dateLong", ts1.equals(tsTest));

        ts1 = value1.getTimestamp("dateString");
        assertTrue("Wrong Timestamp value of dateLong", ts1.equals(tsTest));
    }

    /**
     * Test parse a Timestamp string with zone offset.
     */
    public void testTimestampParseStringWithZoneOffset() {
        String[] strs = new String[] {
            "2017-12-05Z",
            "2017-12-05T01Z",
            "2017-12-05T1:2:0Z",
            "2017-12-05T01:02:03Z",
            "2017-12-05T01:02:03.123456789Z",
            "2017-12-05+01:00",
            "2017-12-05-02:01",
            "2017-12-05T12:39+00:00",
            "2017-12-05T01:02:03+00:00",
            "2017-12-05T01:02:03.123+00:00",
            "2017-12-01T01:02:03+02:01",
            "2017-12-01T01:02:03.123456789+02:01",
            "2017-12-01T01:02:03.0-03:00",
        };

        String[] expStrings = new String[] {
            "2017-12-05T00:00:00",
            "2017-12-05T01:00:00",
            "2017-12-05T01:02:00",
            "2017-12-05T01:02:03",
            "2017-12-05T01:02:03.123456789",
            "2017-12-04T23:00:00",
            "2017-12-05T02:01:00",
            "2017-12-05T12:39:00",
            "2017-12-05T01:02:03",
            "2017-12-05T01:02:03.123",
            "2017-11-30T23:01:03",
            "2017-11-30T23:01:03.123456789",
            "2017-12-01T04:02:03",
        };

        for (int i = 0; i < strs.length; i++) {
            Timestamp ts = parseString(strs[i]);
            String ret = formatString(ts);
            assertEquals(expStrings[i], ret);
        }
    }

    /**
     * Test parse a Timestamp string in format of specified pattern
     */
    @Test
    public void testTimestampParseStringWithPattern() {
        final String[] strs = new String[] {
            "July 7 16 08-29-36-71",
            "02001.July.04 AD 12:08 PM",
            "Wed, Jul 4, '01 12:01"
        };

        final String[] patterns = new String[] {
            "MMMM d yy HH-mm-ss-SS",
            "yyyyy.MMMM.dd G hh:mm a",
            "EEE, MMM d, ''yy HH:mm"
        };

        int i = 0;
        for (String str : strs) {
            String pattern = patterns[i++];
            TimestampValue ts1 = new TimestampValue(str, pattern, true);
            TimestampValue ts2 = new TimestampValue(str, pattern, false);
            assertTrue(ts1.getLong() - ts2.getLong() ==
                       TimeZone.getDefault().getOffset(ts2.getLong()));
        }

        final String[] strsWithZone = new String[] {
            "2001.07.04 AD at 12:08:56 PDT",
            "Wed, 4 Jul 2001 12:08:56.999 -0700",
            "010704120856-0700",
            "2001-07-04T12:08:56.235-0700",
            "July 9 16 Asia/Shanghai"
        };
        final String[] patternsWithZone = new String[] {
            "yyyy.MM.dd G 'at' HH:mm:ss z",
            "EEE, d MMM yyyy HH:mm:ss.SSS Z",
            "yyMMddHHmmssZ",
            "yyyy-MM-dd'T'HH:mm:ss.SSSZ",
            "MMMM d yy VV"
        };

        i = 0;
        for (String str : strsWithZone) {
            String pattern = patternsWithZone[i++];
            TimestampValue ts1 = new TimestampValue(str, pattern, true);
            TimestampValue ts2 = new TimestampValue(str, pattern, false);
            assertEquals(ts1.getTimestamp(), ts2.getTimestamp());
        }
    }

    @Test
    public void testMiscTypes() {
        NullValue val = NullValue.getInstance();
        roundTrip(val);
        EmptyValue emptyVal = EmptyValue.getInstance();
        roundTrip(emptyVal);
    }

    @Test
    public void testNumber() {
        int[] ints = new int[] {
            Integer.MAX_VALUE, Integer.MIN_VALUE, 0, -1234567, 1234567
        };
        for (int ival : ints) {
            NumberValue nv = new NumberValue(BigDecimal.valueOf(ival));
            assertTrue(nv.compareTo(new IntegerValue(ival)) == 0);
            roundTrip(nv);
        }

        /* Long */
        long[] longs = new long[] {
            Long.MIN_VALUE, Long.MAX_VALUE, 0L, -1234567890123456789L,
            1234567890123456789L
        };
        for (long lval : longs) {
            NumberValue nv = new NumberValue(BigDecimal.valueOf(lval));
            assertTrue(nv.compareTo(new LongValue(lval)) == 0);
            roundTrip(nv);
        }

        /* Double */
        double[] doubles = new double[] {
            Double.MIN_VALUE, Double.MAX_VALUE, 0.0d, -1.1231421132132132d,
            132124.132132132132d
        };
        for (double dval : doubles) {
            NumberValue nv = new NumberValue(BigDecimal.valueOf(dval));
            roundTrip(nv);
        }

        /* Number */
        BigDecimal[] decs = new BigDecimal[] {
            BigDecimal.ZERO,
            new BigDecimal(new BigInteger("-9999999999"), Integer.MIN_VALUE + 10),
            new BigDecimal(new BigInteger("9999999999"), Integer.MIN_VALUE + 10),
            new BigDecimal("1.23456789E+1024"),
            new BigDecimal(new BigInteger("9999999999"), Integer.MAX_VALUE)
        };

        NumberValue prev = null;
        for (BigDecimal val : decs) {
            NumberValue nv = new NumberValue(val);
            if (prev != null) {
                int ret = nv.getValue().compareTo(prev.getValue());
                if (ret < 0) {
                    assertTrue(nv.compareTo(prev) < 0);
                } else if (ret > 0) {
                    assertTrue(nv.compareTo(prev) > 0);
                } else {
                    assertTrue(nv.compareTo(prev) == 0);
                }
            }
            prev = nv;
            roundTrip(nv);
        }
    }

    @Test
    public void testBase64EncodeDecode() {
        final int[] sizes = new int[] {0, 9, 32, 76, 100, 1024, 10240};
        for (int size : sizes) {
            byte[] bytes0 = genBytes(size);
            String enc = BinaryValue.encodeBase64(bytes0);
            byte[] bytes1 = BinaryValue.decodeBase64(enc);
            assertTrue(Arrays.equals(bytes0, bytes1));
        }
    }

    /*
     * Test JsonOptions.numericAsNumber used when parses JSON string to MapValue.
     *
     * If JsonOptions.numericAsNumber is true, then all numeric type values
     * are represented using NumberValue. Otherwise, use specific numeric type
     * values.
     */
    @Test
    public void testNumericJsonOptions() {

        int[] ints = new int[] {
            Integer.MIN_VALUE,
            Integer.MAX_VALUE,
            0,
            123456,
        };

        long [] longs = new long[] {
            Long.MIN_VALUE,
            Long.MAX_VALUE,
            0L,
            987654321012345678L,
        };

        float[] flts = new float[] {
            Float.MIN_VALUE,
            Float.MAX_VALUE,
            0.0f,
            1.234567f
        };

        double[] dbls = new double[] {
            Double.MIN_VALUE,
            Double.MAX_VALUE,
            0.0d,
            9.8765432123456d
        };

        for (int i = 0; i < ints.length; i++) {
            runTestJsonWithNumeric(ints[i], longs[i], flts[i], dbls[i], false);
            runTestJsonWithNumeric(ints[i], longs[i], flts[i], dbls[i], true);
        }
    }

    private void runTestJsonWithNumeric(int i,
                                        long l,
                                        float f,
                                        double d,
                                        boolean numericAsNumber) {

         final MapValue base = new MapValue()
            .put("id", 1)
            .put("i", i)
            .put("l", l)
            .put("f", f)
            .put("d", d);

         final JsonOptions options = new JsonOptions()
             .setNumericAsNumber(numericAsNumber);
         final String json = base.toJson(options);
         MapValue value = (MapValue)JsonUtils.createValueFromJson(json, options);
         checkValue(value, i, l, f, d, numericAsNumber);
    }

    private void checkValue(MapValue value,
                            int i,
                            long l,
                            float f,
                            double d,
                            boolean numericAsNumber) {

        if (!numericAsNumber) {
            assertType(value.get("i"), Type.INTEGER);
            assertTrue("Wrong value of \"i\"", value.getInt("i") == i);

            if (l == (int)l) {
                assertType(value.get("l"), Type.INTEGER);
                assertTrue("Wrong value of \"l\"", value.getInt("l") == l);
            } else {
                assertType(value.get("l"), Type.LONG);
                assertTrue("Wrong value of \"l\"", value.getLong("l") == l);
            }

            assertType(value.get("f"), Type.DOUBLE);
            assertTrue("Wrong value of \"f\"",
                       Double.compare(value.getDouble("f"), f) == 0);

            assertType(value.get("d"), Type.DOUBLE);
            assertTrue("Wrong value of \"d\"",
                       Double.compare(value.getDouble("d"), d) == 0);
        } else {
            BigDecimal val;

            assertType(value.get("i"), Type.NUMBER);
            val = value.getNumber("i");
            assertTrue("Wrong value of \"i\"",
                       val.compareTo(BigDecimal.valueOf(i)) == 0);

            assertType(value.get("l"), Type.NUMBER);
            val = value.getNumber("l");
            assertTrue("Wrong value of \"l\"",
                       val.compareTo(BigDecimal.valueOf(l)) == 0);

            assertType(value.get("f"), Type.NUMBER);
            val = value.getNumber("f");
            assertTrue("Wrong value of \"f\"",
                       val.compareTo(BigDecimal.valueOf(f)) == 0);

            assertType(value.get("d"), Type.NUMBER);
            val = value.getNumber("d");
            assertTrue("Wrong value of \"d\"",
                       val.compareTo(BigDecimal.valueOf(d)) == 0);
        }
    }

    @Test
    public void testSpecialCharacter() {
        String[] strs = new String[] {
            "\"", "\"\"", "\t", "\n", "\r", "\\", " ", "'", "a\"b\"c", "a\\c\\"
        };
        MapValue val0 = new MapValue();
        for (String str : strs) {
            val0.put(str, str);
        }

        MapValue val1 = MapValue.createFromJson(val0.toJson(), null).asMap();
        assertEquals(val0, val1);
        for (String str : strs) {
            assertEquals(str, val1.get(str).getString());
        }
    }

    /**
     * Test FieldValue.getSerializedSize()
     * NOTE: the sizes asserted are just those that are calculated and
     * not cast in stone.
     */
    @Test
    public void testSize() {
        final String tsText = "2017-07-15T15:18:59.123456789";
        final String jsonMap = "{\"a\":1, \"b\": \"def\"}";
        final String jsonArray = "[1, 2, 3, 4, 5]";
        FieldValue val;

        val = new IntegerValue(0);
        assertSize(val, 2);

        val = new IntegerValue(Integer.MAX_VALUE);
        assertSize(val, 6);

        val = new LongValue(0);
        assertSize(val, 2);

        val = new LongValue(Long.MAX_VALUE);
        assertSize(val, 10);

        val = new DoubleValue(0.0);
        assertSize(val, 9);

        val = new DoubleValue(Double.NaN);
        assertSize(val, 9);

        val = new NumberValue(new BigDecimal(Integer.MAX_VALUE));
        assertSize(val, 12);

        val = new TimestampValue(tsText);
        assertSize(val, 31);

        val = BooleanValue.trueInstance();
        assertSize(val, 2);

        val = new BinaryValue(new byte[56]);
        assertSize(val, 58);

        val = new StringValue("abcdefg");
        assertSize(val, 9);

        val = MapValue.createFromJson(jsonMap, null);
        assertSize(val, 20);

        val = ArrayValue.createFromJson(jsonArray, null);
        assertSize(val, 19);
    }

    @Test
    public void testFieldCreator() throws Exception {
        boolean debug = false;

        BinaryProtocol.FieldValueCreator creator =
            new BinaryProtocol.FieldValueCreator();

        /*
         * This test just creates some values and ensures that they can
         * be turned into JSON. If there are any bugs found in this code
         * test cases can be added here.
         */

        /*
         * Nested maps
         */
        creator.startMap(1);
        creator.startMapField("a");
        creator.startMap(0);
        creator.endMap(0);
        creator.endMapField();
        creator.endMap(1);
        String json = creator.getCurrentValue().toJson(JsonOptions.PRETTY);
        if (debug) {
            System.out.println(json);
        }

        /*
         * Nested array
         */
        creator = new BinaryProtocol.FieldValueCreator();
        creator.startArray(8);

        creator.startMap(3);
        creator.startMapField("a");
        creator.integerValue(5);
        creator.endMapField();
        creator.startMapField("b");
        creator.jsonNullValue();
        creator.endMapField();
        creator.endMap(3);
        creator.endArrayField();

        for (int i = 0; i < 6; i++) {
            creator.integerValue(i);
            creator.endArrayField();
        }

        creator.startArray(1);
        creator.stringValue("abcde");
        creator.endArrayField();
        creator.endArray(1);
        creator.endArrayField();

        creator.endArray(8);
        json = creator.getCurrentValue().toJson(JsonOptions.PRETTY);
        if (debug) {
            System.out.println(json);
        }
    }

    private void assertSize(FieldValue val, int size) {
        /* Leave for future debugging:
         * System.out.println("assertSize: " + val + ", " +
         * val.getSerializedSize());
         */
        assertEquals(size, val.getSerializedSize());
    }

    private void assertType(FieldValue value, FieldValue.Type type) {
        assertTrue("Wrong type. expect " + type + " actual " + value.getType(),
            value.getType() == type);
    }

    private byte[] genBytes(int length) {
        byte[] bytes = new byte[length];
        for (int i = 0; i < length; i++) {
            bytes[i] = (byte)(i % 256);
        }
        return bytes;
    }

    /*
     * Local utilities
     */

    private void expectParseError(String doc, JsonOptions options) {
        try {
            FieldValue.createFromJson(doc, options);
            fail(("Expected parse error for doc: " + doc));
        } catch (JsonParseException jpe) {
            // success
        }
    }

    private void expectParseException(String str, JsonOptions options) {
        try {
            JsonUtils.createValueFromJson(str, options);
            fail(("Expected parse error for str: " + str));
        } catch (JsonParseException jpe) {
            // success
        }
    }

    private void expectParseError(String doc,
                                  JsonOptions options,
                                  int line,
                                  int column) {
        try {
            FieldValue.createFromJson(doc, options);
            fail(("Expected parse error for doc: " + doc));
        } catch (JsonParseException jpe) {
            assertEquals("Unexpected error line", line, jpe.getLine());
            /*
             * For some reason the columns are off relative to what is expected.
             * Some research into how Jackson gets this number needs to be done.
            assertEquals("Unexpected error column", column, jpe.getColumn());
            */
        }
    }

    private void assertTypeFromJson(String doc,
                                    FieldValue.Type type,
                                    JsonOptions options) {
        FieldValue val = FieldValue.createFromJson(doc, options);
        assertEquals("Unexpected type", type, val.getType());
        /* test serialization as well */
        roundTrip(val);
    }

    private void roundTrip(FieldValue value) {

        try {
            ByteBuf buf = Unpooled.buffer();
            ByteOutputStream bos = new ByteOutputStream(buf);
            BinaryProtocol.writeFieldValue(bos, value);
            ByteInputStream bis = new ByteInputStream(buf);
            FieldValue newValue = BinaryProtocol.readFieldValue(bis);
            assertEquals(value, newValue);

            /*
             * exercise event generation and construction here
             */
            BinaryProtocol.FieldValueCreator creator =
                new BinaryProtocol.FieldValueCreator();
            FieldValueEventHandler.generate(value, creator);
            assertEquals(value, creator.getCurrentValue());
        } catch (Exception e) {
            fail("Exception in roundTrip: " + e);
        }
    }
}
