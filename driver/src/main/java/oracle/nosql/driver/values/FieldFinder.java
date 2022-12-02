/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.values;

import java.io.IOException;

import oracle.nosql.driver.Nson;
import oracle.nosql.driver.util.ByteInputStream;

/**
 * @hidden
 * An instance of {@link FieldValueEventHandler} that finds a specific
 * field in NSON by its path where the path is a "." separated string, e.g
 *   a.b.c
 * At this time it will only return a single FieldValue and navigation
 * through arrays is not supported. Future enhancements should consider
 * multiple values and array navigation.
 * <p>
 * If found the FieldValue can be returned using {@link #getTargetValue}
 * and the return value of {@link #getFound} will be true.
 * <p>
 * There are a few ways to use the class
 * <ul>
 * <li>
 *  Call one of the static find methods:
 * {@link #find(ByteInputStream, String)} or {@link #find(MapValue, String)}
 * </li>
 * <li>
 * Construct an instance and pass it to code that can generate events
 * to be consumed by an instance of {@link FieldValueEventHandler}
 * </li>
 * </ul>
 * <p>
 * TODO, or think about
 * <ul>
 * <li>
 * returning primitive values vs FieldValue, e.g. findInteger()
 * or findObject() and have the caller cast to a primitive (Integer,
 * String, Long, Boolean, etc). This avoids object creation for primitives.
 * This would be done by overriding the primitive events in the parent class
 * and intead of setting the currentValue it'd set an Object. It'd also
 * affect checkTargetValue. Not difficult to do at all.
 * </li>
 * <li>eliminate objects created in FieldValueCreator</li>
 * <li>wild cards and multiple results (*.b.c)</li>
 * <li>array nav (a.b[].c, a.b[5], etc)</li>
 * <li>use our own JSON query navigational syntax</li>
 * <li>
 * consider paths with keys that include "." e.g. "a.b".c where
 * "a.b" is a single field name
 * </li>
 * </ul>
 */
public class FieldFinder extends Nson.FieldValueCreator {

    private String[] path;
    private int currentIndex;
    /*
     * inTarget is true when the target field is started. It may be a leaf
     * or an object or array
     */
    private boolean inTarget = false;

    /* depth is  nesting depth of the current find operation, starting at 0 */
    private int depth;

    /* targetDepth is the nesting depth of the target, set with inTarget */
    private int targetDepth;

    /* found indicates if the target was found or not */
    private boolean found = false;

    /*
     * createTargetValue will be set to true if the value object of the target
     * is to be created. For example if doing a seek this will not be true
     */
    private boolean createTargetValue = false;

    /* targetValue is created if it is found and should be created */
    private FieldValue targetValue;

    /* set to true if only seeking to target */
    private boolean doSeek = false;

    /*
     * set to true if event generation should stop. This happens if (1) seek
     * has found the target or (2) target has been found and created
     */
    private boolean stopEvents = false;

    /**
     * Looks for a path in an NSON stream.
     * @param bis a stream of NSON that must start with a MAP. If it is not a
     * map it is not an error but key values are only available in an NSON MAP
     * @param path a string path of the format field1[.fieldN]* representing
     * the target field in the NSON
     * @return a {@link FieldValue} representing the target field or null
     * if it is not found
     */
    public static FieldValue find(ByteInputStream bis, String path) {
        FieldFinder finder = new FieldFinder(path);
        return finder.find(bis);
    }

    /**
     * Looks for a path in a MapValue.
     * @param map the map to search
     * @param path a string path of the format field1[.fieldN]* representing
     * the target field in the map
     * @return a {@link FieldValue} representing the target field or null
     * if it is not found
     */
    public static FieldValue find(MapValue map, String path) {
        FieldFinder finder = new FieldFinder(path);
        return finder.find(map);
    }

    /**
     * Construct an instance to find the specified path of the form
     * field1[.fieldN]*
     * @param path the path
     */
    public FieldFinder(String path) {
        this.path = path.split("\\.");
        currentIndex = 0;
    }

    /**
     * Resets the finder without creating a new instance
     * @param path the new path
     */
    public void reset(String path) {
        this.path = path.split("\\.");
        currentIndex = 0;
        inTarget = false;
        found = false;
        createTargetValue = false;
        targetValue = null;
        doSeek = false;
        stopEvents = false;
        targetDepth = 0;
        depth = 0;
    }

    /**
     * Looks for a the current path in an NSON stream.
     * @param bis a stream of NSON that must start with a MAP. If it is not a
     * map it is not an error but key values are only available in an NSON MAP
     * @return a {@link FieldValue} representing the target field or null
     * if it is not found
     */
    public FieldValue find(ByteInputStream bis) {
        try {
            Nson.generateEventsFromNson(this, bis, false);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Failure looking for path " + path + " in NSON stream: " +
                ioe.getMessage(), ioe);
        }
        return getTargetValue();
    }

    /**
     * Looks for the current path in a MapValue.
     * @param map the map to search
     * @return a {@link FieldValue} representing the target field or null
     * if it is not found
     */
    public FieldValue find(MapValue map) {
        try {
            FieldValueEventHandler.generate(map, this, false);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Failure looking for path " + path + " in MapValue: " +
                ioe.getMessage(), ioe);
        }
        return getTargetValue();
    }

    /**
     * Looks for a the current path in an NSON stream but only positions the
     * stream at the value of the target path. This allows callers to avoid
     * creation of FieldValue instances based on the target value, which may
     * be atomic or complex.
     *
     * @param path the path to seek; this will reset the internal path
     * @param bis a stream of NSON that must start with a MAP. If it is not a
     * map it is not an error but key values are only available in an NSON MAP
     * @return true if the target was found, false otherwise. If the target is
     * found the stream is positioned at the target value.
     */
    public boolean seek(String path, ByteInputStream bis) {
        reset(path);
        return seek(bis);
    }

    /**
     * Looks for a the current path in an NSON stream but only positions the
     * stream at the value of the target path. This allows callers to avoid
     * creation of FieldValue instances based on the target value, which may
     * be atomic or complex.
     *
     * @param bis a stream of NSON that must start with a MAP. If it is not a
     * map it is not an error but key values are only available in an NSON MAP
     * @return true if the target was found, false otherwise. If the target is
     * found the stream is positioned at the target value.
     */
    public boolean seek(ByteInputStream bis) {
        doSeek = true;
        try {
            Nson.generateEventsFromNson(this, bis, false);
        } catch (IOException ioe) {
            throw new IllegalArgumentException(
                "Failure looking for path " + path + " in NSON stream: " +
                ioe.getMessage(), ioe);
        }
        return inTarget;
    }

    /**
     * Returns the target value if the path was found, null otherwise
     * @return the found value
     */
    public FieldValue getTargetValue() {
        return targetValue;
    }

    /**
     * Returns true if the path was found
     * @return true if found
     */
    public boolean getFound() {
        return found;
    }

    @Override
    public void startMap(int size) throws IOException {
        ++depth;
        if (inTarget) {
            super.startMap(size);
            checkTargetValue();
        }
    }

    @Override
    public void endMap(int size) throws IOException {
        --depth;
        if (inTarget) {
            super.endMap(size);
        }
    }

    @Override
    public boolean startMapField(String key) throws IOException {
        if (inTarget ||
            (currentIndex + 1 == depth &&
             key.equals(this.path[currentIndex]))) {
            currentIndex++;
            /*
             * This must be the last portion of the path expression AND
             * the depth must be correct (the same as the index at this
             * point) to make this the target field
             */
            if (currentIndex == this.path.length &&
                currentIndex == depth) {
                if (!inTarget) {
                    inTarget = true;
                    if (doSeek) {
                        stopEvents = true;
                    }
                    targetDepth = depth;
                    createTargetValue = true;
                }
            }
            /* if stopping events super.startMapField is unnecessary overhead */
            if (!stopEvents) {
                return super.startMapField(key);
            }
        }
        return true; // skip
    }

    @Override
    public void endMapField(String key) throws IOException {
        boolean wasInTarget = inTarget;
        if (targetDepth == depth) {
            inTarget = false;
            found = true;
            stopEvents = true;
        } else if (currentIndex > depth) {
            /*
             * If we are navigating out of a nested field that was
             * part of the path desired and haven't yet found the
             * target, we never will, so stop looking
             */
            stopEvents = true;
        }
        if (wasInTarget) {
            super.endMapField(key);
            checkTargetValue();
        }
    }

    @Override
    public void startArray(int size) throws IOException {
        if (inTarget) {
            super.startArray(size);
            checkTargetValue();
        }
    }

    @Override
    public void endArray(int size) throws IOException {
        if (inTarget) {
            super.endArray(size);
            checkTargetValue();
        }
    }

    @Override
    public void endArrayField(int index) throws IOException {
        if (inTarget) {
            super.endArrayField(index);
            checkTargetValue();
        }
    }

    @Override
    public boolean stop() {
        /*
         * Stop events if:
         * 1. doing a seek and target has been found
         * 2. the target has been found and has been created
         */
        return stopEvents;
    }

    /**
     * See if target value needs to be set and if so, set it and clear
     * the flag that indicates it should be set - createTargetValue
     */
    private void checkTargetValue() {
        if (createTargetValue) {
            /*
             * target is the currentValue from super
             */
            assert(targetValue == null);
            targetValue = getCurrentValue();
            createTargetValue = false;
        }
    }

    /**
     * A class that allows a caller to walk an NSON MAP using a "pull"
     * pattern. The ByteInputStream used for construction must be positioned
     * on an NSON MAP. Each call to {@link MapWalker#next} moves the state
     * forward. The value of each map entry <b>must</b> be consumed by
     * the caller either by reading it or skipping it by calling
     * {@link MapWalker#skip}. A typical usage pattern is:
     * <pre>
     *   MapWalker walker = new MapWalker(stream);
     *   while (walker.hasNext()) {
     *       walker.next();
     *       String name = walker.getName(); // returns name of current element
     *       // read or skip value
     *       if (name.equals("interesting_int")) {
     *           Nson.readNsonInt(stream);
     *       else {
     *           walker.skip();
     *       }
     *   }
     * </pre>
     */
    public static class MapWalker {
        /* this exists to prevent an infinite loop for bad serialization */
        private final static int MAX_NUM_ELEMENTS = 1_000_000_000;
        private final ByteInputStream bis;
        private final int numElements;
        private String currentName;
        private int currentIndex = 0;

        /**
         * Constructs a MapWalker from a stream
         * @param bis a ByteInputStream
         * @throws IllegalArgumentException if the stream is not positioned
         * on a MAP
         * @throws IOException if there's a problem with the stream
         */
        public MapWalker(ByteInputStream bis) throws IOException {
            this.bis = bis;
            int t = bis.readByte();
            /* must be map */
            if (t != Nson.TYPE_MAP) {
                throw new IllegalArgumentException(
                    "MapWalker: Stream must point to a MAP, it points to " +
                    Nson.typeString(t));
            }
            bis.readInt(); // total length of map in bytes
            numElements = bis.readInt();
            if (numElements < 0 || numElements > MAX_NUM_ELEMENTS) {
                throw new IllegalArgumentException(
                    "Invalid number of map elements: " + numElements);
            }
        }

        /**
         * Returns the name of the current element in the map
         * @return the element name
         */
        public String getCurrentName() {
            return currentName;
        }

        /**
         * Returns the stream being walked
         * @return the stream
         */
        public ByteInputStream getStream() {
            return bis;
        }

        /**
         * Returns true if the map has more elements
         * @return true if the map has more elements, false otherwise
         */
        public boolean hasNext() {
            return (currentIndex < numElements);
        }

        /**
         * Moves the state forward to the next element in the map.
         * @throws IllegalArgumentException if called when the map has
         * no more elements
         * @throws IOException if there's a problem with the stream
         */
        public void next() throws IOException {
            if (currentIndex >= numElements) {
                throw new IllegalArgumentException(
                    "Cannot call next with no elements remaining");
            }
            currentName = Nson.readString(bis);
            currentIndex++;
        }

        /**
         * Skips the value of the element at which the stream is positioned.
         * If called when the map has no further elements or without calling
         * {@link #next} the result is unpredictable and the stream state
         * will be undefined.
         * @throws IOException if there's a problem with the stream
         */
        public void skip() throws IOException {
            Nson.generateEventsFromNson(null, bis, true);
        }
    }
}
