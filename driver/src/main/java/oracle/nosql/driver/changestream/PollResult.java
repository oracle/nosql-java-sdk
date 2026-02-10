/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.changestream;

import oracle.nosql.driver.ops.Result;

/*
 * @hidden
 * Internal result for Consumer poll() operations
 */
public class PollResult extends Result {
    public MessageBundle bundle;
    public byte[] cursor;
    public Consumer consumer;
    public long eventsRemaining;
}

