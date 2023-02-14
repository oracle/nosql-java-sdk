/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
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
 * DefinedTags is a class to encapsulate defined tags which are returned
 * from calls to {@link NoSQLHandle#getTable}. They can also be set during
 * table creation operations as well as alter table operations.
 * @since 5.4
 */
public class DefinedTags {
    private final MapValue tags;

    /**
     * Creates a new instance of DefinedTags. This method along with
     * {@link #addTag} is used to add tags to a call to create or modify
     * a table.
     */
    public DefinedTags() {
        tags = new MapValue();
    }

    /**
     * Creates a new instance of DefinedTags from JSON string input.
     * @param jsonString a JSON string
     */
    public DefinedTags(String jsonString) {
        tags = (MapValue) FieldValue.createFromJson(jsonString, null);
    }

    /**
     * Adds a new tag to the set of tags in the specified namespace
     *
     * @param namespace the namespace for the tag
     * @param key the tag key
     * @param value the tag value
     * @return this
     */
    public DefinedTags addTag(String namespace, String key, String value) {
        if (!tags.contains(namespace)) {
            tags.put(namespace, new MapValue());
        }
        MapValue nsMap = (MapValue) tags.get(namespace);
        nsMap.put(key, value);
        return this;
    }

    /**
     * Returns the value of the named tag in the specified namespace or null
     * if it does not exist
     * @param namespace the namespace
     * @param key the key
     * @return the tag value or null
     */
    public String getTag(String namespace, String key) {
        if (tags.contains(namespace)) {
            FieldValue val = ((MapValue)tags.get(namespace)).get(key);
            return val == null ? null : val.getString();
        }
        return null;
    }

    /**
     * Returns true if the specified key exists in the specified namespace
     * @param namespace the namespace
     * @param key the key
     * @return true if the key exists
     */
    public boolean contains(String namespace, String key) {
        if (tags.contains(namespace)) {
            return ((MapValue)tags.get(namespace)).contains(key);
        }
        return false;
    }

    /**
     * Returns the free-form tags as a JSON string
     * @return the JSON string
     */
    @Override
    public String toString() {
        return tags.toString();
    }
}
