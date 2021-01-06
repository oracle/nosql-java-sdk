/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

/**
 * An extension of the Netty ByteBufInputStream that provides access to methods
 * to get and set offsets into the byte buffer that underlies the stream. This
 * class prevents knowledge of Netty from being required in the serialization
 * code.
 *
 * NOTE: this class implements DataInput
 */
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufInputStream;

public class ByteInputStream extends ByteBufInputStream {
    final ByteBuf buffer;

    public ByteInputStream(ByteBuf buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    /**
     * Returns the current read offset
     */
    public int getOffset() {
        return buffer.readerIndex();
    }

    /**
     * Sets the read offset.
     */
    public void setOffset(int offset) {
        buffer.readerIndex(offset);
    }

    public ByteBuf buffer() {
        return buffer;
    }
}
