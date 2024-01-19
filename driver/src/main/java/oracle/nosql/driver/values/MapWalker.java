/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.values;

import java.io.IOException;

import oracle.nosql.driver.Nson;
import oracle.nosql.driver.util.ByteInputStream;

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
 * @hidden
 */
public class MapWalker {
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
     * Returns the number of elements in the map
     * @return the number of elements
     */
    public int getNumElements() {
        return numElements;
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
