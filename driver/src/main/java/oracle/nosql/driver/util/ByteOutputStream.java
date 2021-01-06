/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

import java.io.IOException;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufOutputStream;
import io.netty.buffer.Unpooled;

/**
 * An extension of the Netty ByteBufOutputStream that provides access to methods
 * to get and set offsets into the byte buffer that underlies the stream. This
 * class prevents knowledge of Netty from being required in the serialization
 * code.
 *
 * NOTE: this class implements DataOutput
 */
public class ByteOutputStream extends ByteBufOutputStream {
    final ByteBuf buffer;

    public ByteOutputStream(ByteBuf buffer) {
        super(buffer);
        this.buffer = buffer;
    }

    /**
     * Creates a ByteOutputStream, also allocating a ByteBuf. This
     * buffer must be released by calling releaseByteBuf.
     */
    public static ByteOutputStream createByteOutputStream() {
        return new ByteOutputStream(Unpooled.buffer());
    }

    public void releaseByteBuf() {
        buffer.release();
    }

    /**
     * Returns the underlying ByteBuf
     */
    public ByteBuf getByteBuf() {
        return buffer;
    }

    /**
     * Returns the current write offset into the byte buffer
     */
    public int getOffset() {
        return buffer.writerIndex();
    }

    /**
     * Sets the current write offset into the byte buffer
     */
    public void setWriteIndex(int index) {
        buffer.writerIndex(index);
    }

    /**
     * Skip numBytes, resetting the offset.
     */
    public void skipBytes(int numBytes) {
        buffer.ensureWritable(numBytes);
        buffer.writerIndex(buffer.writerIndex() + numBytes);
    }

    /**
     * Writes the value at the specified offset. The offset must be less
     * than the current offset.
     */
    public void writeIntAtOffset(int offset, int value) throws IOException {
        int currentOffset = buffer.writerIndex();
        if (offset > currentOffset) {
            throw new IllegalArgumentException(
                "Invalid offset: " + offset +
                " must be less than current offset: " + currentOffset);
        }
        buffer.writerIndex(offset);
        writeInt(value);

        /* reset */
        buffer.writerIndex(currentOffset);

    }

    /**
     * Writes the value at the specified offset. The offset must be less
     * than the current offset.
     */
    public void writeBooleanAtOffset(int offset, boolean value)
        throws IOException {

        int currentOffset = buffer.writerIndex();
        if (offset > currentOffset) {
            throw new IllegalArgumentException(
                "Invalid offset: " + offset +
                " must be less than current offset: " + currentOffset);
        }
        buffer.writerIndex(offset);
        writeBoolean(value);

        /* reset */
        buffer.writerIndex(currentOffset);

    }

    /**
     * Writes the byte array at the specified offset. The offset must be less
     * than the current offset.
     */
    public void writeArrayAtOffset(int offset, byte[] value)
        throws IOException {

        int currentOffset = buffer.writerIndex();
        if ((offset + value.length) > currentOffset) {
            throw new IllegalArgumentException(
                "Invalid offset and length: " + (offset + value.length) +
                " must be less than current offset: " + currentOffset);
        }
        buffer.writerIndex(offset);
        write(value);

        /* reset */
        buffer.writerIndex(currentOffset);

    }
}
