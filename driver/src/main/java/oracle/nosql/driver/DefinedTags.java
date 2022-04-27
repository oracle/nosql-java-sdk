/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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
 * defined tags are always in a namespace
 * each namespace may contain a map of tags where the key is a string and
 * the value is a string, number or boolean.
 * {
 *  "namespace_name" : { "key":"value", ....}
 *  ...
 * }
 *
 * Cloud service only.
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
     * @paran jsonString a JSON string
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
     * @hidden
     * Used for return
     */
    public DefinedTags(MapValue tags) {
        this.tags = tags;
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
