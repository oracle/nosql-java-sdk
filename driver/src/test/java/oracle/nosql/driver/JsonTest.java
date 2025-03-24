/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

import org.junit.Test;

import oracle.nosql.driver.values.JsonOptions;
import oracle.nosql.driver.values.JsonReader;
import oracle.nosql.driver.values.MapValue;

/**
 * A set of test cases to exercise JsonReader and related JSON
 * utilities.
 *
 * TBD: lots more test cases.
 */
public class JsonTest extends DriverTestBase {

    /**
     * tests bad JSON used in JsonReader
     */
    @SuppressWarnings("unused")
    @Test
    public void testBadStrings() {
        final boolean debug = false;
        final String good1 = "{\"a\": 1}";
        /* this is "bad" but will work because the parser skips non-objects */
        final String dicey1 = "xyx 1 {\"a\": 1},{\"b\": false}";

        /* bad object */
        final String bad1 = "{\"a\": 1},{\"b\": false";

        /*
         * parse the string, args are:
         *  json, number of objects expected, should it succeed?, debug print?
         *
         * Note that failed operations can still see a number of valid objects
         * before running into bad JSON.
         */
        parse(good1, 1, true, debug);
        parse(dicey1, 2, true, debug);
        parse(bad1, 1, false, debug);

        /*
         * Test multiple use, which should fail
         */
        try (JsonReader reader = new JsonReader(good1, null)) {
            reader.iterator();
            try {
                reader.iterator();
                fail("Should have thrown IAE");
            } catch (IllegalArgumentException iae) {
                // expected
            }
        }

        try (JsonReader reader = new JsonReader(good1, null)) {
            /* this internally calls iterator() */
            for (MapValue value : reader) {}
            try {
                for (MapValue value : reader) {}
                fail("Should have thrown IAE");
            } catch (IllegalArgumentException iae) {
                // expected
            }
        }
    }

    @Test
    public void testOrder() {
        final String order1 = "{\"1\": 1,\"2\": false}";
        final String order2 = "{\"8\": 1, \"7\": false}";
        JsonOptions options = new JsonOptions().setMaintainInsertionOrder(true);
        try (JsonReader reader1 = new JsonReader(order1, options);
             JsonReader reader2 = new JsonReader(order2, options)) {
            for (MapValue value : reader1) {
                String[] keys = value.getMap().keySet().toArray(new String[0]);
                assertEquals("1", keys[0]);
                assertEquals("2", keys[1]);
            }
            for (MapValue value : reader2) {
                String[] keys = value.getMap().keySet().toArray(new String[0]);
                assertEquals("8", keys[0]);
                assertEquals("7", keys[1]);
            }
        }
    }


    private static void parse(String json,
                              int numObjects,
                              boolean expectSuccess,
                              boolean debugPrint) {
        /* try-with-resources to close parser automatically */
        try (JsonReader reader = new JsonReader(json, null)) {
            debugReader(reader, numObjects, expectSuccess, debugPrint);
        }
    }

    private static void debugReader(JsonReader reader,
                                    int numObjects,
                                    boolean expectSuccess,
                                    boolean debugPrint) {
        int count = 0;
        try {
            for (MapValue value : reader) {
                if (debugPrint) {
                    System.out.println(value);
                }
                ++count;
            }
        } catch (JsonParseException jpe) {
            if (expectSuccess) {
                fail("Parse failed and expected success: " + jpe.getMessage());
            }
        }
        assertEquals("Unexpected count of objects", numObjects, count);
    }
}
