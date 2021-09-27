/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

import java.io.DataInput;

/**
 * An extension of DataInput that provides access to methods
 * to get and set offsets into the byte buffer that underlies the stream.
 */

public interface ByteInputStream extends DataInput, AutoCloseable {

    /**
     * Returns the current read offset
     * @return the offset
     */
    public int getOffset();

    /**
     * Sets the read offset. It can only be set smaller than the current
     * offset
     * @param offset the offset
     * @throws IllegalArgumentException if the offset exceeds the current offset
     */
    public void setOffset(int offset);

    /**
     * Skips the specified number of bytes
     * @param toSkip the number of bytes
     * @throws IllegalArgumentException if toSkip exceeds the capacity of
     * the underlying stream
     */
    public void skip(int toSkip);

    /**
     * This override avoids the default signature of Closeable that throws
     * Exception
     */
    @Override
    public void close();
}
