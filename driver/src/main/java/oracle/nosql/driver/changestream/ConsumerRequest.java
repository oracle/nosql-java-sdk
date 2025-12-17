/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
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
 * Internal request class used for Change Streaming operations
 */
public class ConsumerRequest extends Request {

    /* Builder is used when creating/updating a consumer. */
    public ConsumerBuilder builder;

    public static enum RequestMode {
        UNINITIALIZED(0),
        CREATE(1),
        UPDATE(2),
        CLOSE(3),
        DELETE(4),
        COMMIT(5),
        RESET(6);

        RequestMode(int i) {
            //TODO Auto-generated constructor stub
        }
    }

    // Mode specifies the mode of the request (create, close, delete, etc)
    public RequestMode mode;

    // cursor is used when closing a consumer
    public byte[] cursor;

    public ConsumerRequest(RequestMode mode) {
        this.mode = mode;
    }

    /*
     * @hidden
     */
    public ConsumerRequest setBuilder(ConsumerBuilder builder) {
        this.builder = builder;
        return this;
    }

    /*
     * @hidden
     */
    public ConsumerRequest setCursor(byte[] cursor) {
        this.cursor = cursor;
        return this;
    }

    /*
     * @hidden
     */
    public ConsumerRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /*
     * @hidden
     */
    public ConsumerRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    @Override
    public void validate() {
        /* TODO */
    }

    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createConsumerSerializer();
    }

    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createConsumerDeserializer();
    }

    @Override
    public String getTypeName() {
        return "Consumer";
    }

    @Override
    public boolean shouldRetry() {
        return false;
    }
}
