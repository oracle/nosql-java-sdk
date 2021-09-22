/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

/**
 * An implementation of ByteInputStream that uses Netty's
 * ByteBufInputStream
 */
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

public class NettyByteInputStream extends ByteBufInputStream
    implements ByteInputStream {

    final ByteBuf buffer;

    /**
     * Creates an instance using a (Netty) ByteBuf
     * @param buffer the buffer
     */
    public NettyByteInputStream(ByteBuf buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    @Override
    public int getOffset() {
        return buffer.readerIndex();
    }

    @Override
    public void setOffset(int offset) {
        buffer.readerIndex(offset);
    }

    @Override
    public void skip(int toSkip) {
        setOffset(getOffset() + toSkip);
    }

    /**
     * Returns the underlying (Netty) ByteBuf
     * @return the buffer
     */
    public ByteBuf buffer() {
        return buffer;
    }

    @Override
    public void close() {
        try {
            super.close();
        } catch (Exception e) {}
    }
}
