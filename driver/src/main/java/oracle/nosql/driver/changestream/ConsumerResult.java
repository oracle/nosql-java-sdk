/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.changestream;

import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.values.MapValue;

/*
 * @hidden
 * Internal result for Consumer operations
 */
public class ConsumerResult extends Result {

    public byte[] cursor;
    public MapValue metadata;
}

