/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.cdc;

import oracle.nosql.driver.values.MapValue;

/**
 * The base class that contains the actual record data.
 * It consists of the record value and record metadata.
 */
public class Image {
    private MapValue value;
    private MapValue metadata;

    /*
     * @hidden
     */
    public Image() {}

    /*
     * @hidden
     */
    public Image(MapValue value, MapValue metadata) {
        value = value;
        metadata = metadata;
    }

    public MapValue getValue() {
        return value;
    }

    public MapValue getMetadata() {
        return metadata;
    }

    /*
     * @hidden
     */
    public void setValue(MapValue value) {
        this.value = value;
    }

    /*
     * @hidden
     */
    public void setMetadata(MapValue metadata) {
        this.metadata = metadata;
    }

    /*
     * @hidden
     */
    public boolean isEmpty() {
        return (value == null && metadata == null);
    }
}
