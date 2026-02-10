/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.changestream;

import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;

/*
 * @hidden
 * Internal request class used for Change Streaming poll operations
 */
public class PollRequest extends Request {

    public byte[] cursor;
	public int limit;

    public PollRequest(byte[] cursor, int limit) {
        this.cursor = cursor;
        this.limit = limit;
    }

    public PollRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    @Override
    public void validate() {
        /* TODO */
    }

    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createPollSerializer();
    }

    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createPollDeserializer();
    }

    @Override
    public String getTypeName() {
        return "Poll";
    }

    @Override
    public boolean shouldRetry() {
        return false;
    }
}
