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
    private MapValue recordValue;
    private MapValue recordMetadata;

    Image(MapValue value, MapValue metadata) {
        recordValue = value;
        recordMetadata = metadata;
    }

    public MapValue getValue() {
        return recordValue;
    }

    public MapValue getMetadata() {
        return recordMetadata;
    }
}
