/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import java.util.Iterator;
import java.util.Map;

import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;

/**
 * Cloud service only.
 *
 * FreeFormTags is a class to encapsulate free-form tags which are returned
 * from calls to {@link NoSQLHandle#getTable}. They can also be set during
 * table creation operations as well as alter table operations.
 * @since 5.4
 */
public class FreeFormTags implements Iterable<Map.Entry<String, FieldValue>> {
    private final MapValue tags;

    /**
     * Creates a new instance of FreeFormTags. This method along with
     * {@link #addTag} is used to add tags to a call to create or modify
     * a table.
     */
    public FreeFormTags() {
        tags = new MapValue();
    }

    /**
     * Creates a new instance of FreeFormTags from JSON string input.
     * @param jsonString a JSON string
     */
    public FreeFormTags(String jsonString) {
        tags = (MapValue) FieldValue.createFromJson(jsonString, null);
    }

    /**
     * Creates a new instance of FreeFormTags from a Map
     * @param map the map
     */
    public FreeFormTags(Map<String, String> map) {
        tags = new MapValue();
        for (Map.Entry<String, String> entry : map.entrySet()) {
            tags.put(entry.getKey(), entry.getValue());
        }
    }

    /**
     * Adds a new tag to the set of tags
     *
     * @param key the tag key
     * @param value the tag value
     * @return this
     */
    public FreeFormTags addTag(String key, String value) {
        tags.put(key, value);
        return this;
    }

    /**
     * Returns the value of the named tag or null if it does not exist
     * @param key the key
     * @return the tag value or null
     */
    public String getTag(String key) {
        FieldValue val = tags.get(key);
        return val == null ? null : val.getString();
    }

    /**
     * Returns the number of tags in the set
     *
     * @return the size
     */
    public int size() {
        return tags.size();
    }

    /**
     * Returns true if the specified key exists in the tags map
     * @param key the key
     * @return true if the key exists
     */
    public boolean contains(String key) {
        return tags.getMap().containsKey(key);
    }

    /**
     * Returns the free-form tags as a JSON string
     * @return the JSON string
     */
    @Override
    public String toString() {
        return tags.toString();
    }

    @Override
    public Iterator<Map.Entry<String, FieldValue>> iterator() {
        return tags.iterator();
    }
}
