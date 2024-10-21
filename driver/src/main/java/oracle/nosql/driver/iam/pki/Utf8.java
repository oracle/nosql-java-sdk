/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.iam.pki;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Mutable byte buffer for UTF-8 encoded text. When the text has been consumed it MUST be erased via
 * {@link #close()} to avoid leaking sensitive data on the heap.
 */
interface Utf8 extends CharSequence, Sensitive {

    /**
     * Wrap a mutable byte array. Does not claim ownership of the array. Caller is responsible for
     * erasing the byte array
     *
     * @param bytes The bytes to wrap
     * @return Utf8 instance
     */
    static Utf8 of(byte[] bytes) {
        return new Chars(CharBuffer.wrap(Text.chars(bytes)));
    }

    /**
     * Buffer the contents of a stream into a Utf8 buffer. The contents will be erased when the
     * buffer is closed.
     *
     * @param content The content to buffer
     * @return Utf8 buffer
     * @throws IOException
     */
    static Utf8 of(ReadableByteChannel content) throws IOException {
        try (final ByteArrayOutputStream bytes = new ByteArrayOutputStream();
                final WritableByteChannel sink = Channels.newChannel(bytes)) {
            final ByteBuffer buffer = ByteBuffer.allocate(4096);
            while (content.read(buffer) != -1) {
                buffer.flip();
                while (buffer.hasRemaining()) {
                    /* write() might not write all of the bytes in a single pass */
                    sink.write(buffer);
                }
                buffer.clear();
            }
            buffer.flip();
            while (buffer.hasRemaining()) {
                sink.write(buffer);
            }
            final byte[] buffered = bytes.toByteArray();
            try {
                return Utf8.of(buffered);
            } finally {
                Eraser.erase(buffer);
                Eraser.erase(buffered);
            }
        }
    }

    /**
     * Wrap an immutable String.
     *
     * @param text The string to wrap
     * @return Utf8 instance
     */
    static Utf8 of(final String text) {
        return new Wrapper(text);
    }

    @Override
    Utf8 subSequence(int start, int end);

    default boolean contains(final String text) {
        return indexOf(text) != -1;
    }

    default int indexOf(String text) {
        return indexOf(text, 0);
    }

    default int indexOf(String text, int offset) {
        int fromIndex = offset;
        int sourceCount = length();
        int targetCount = text.length();
        int targetOffset = 0;
        int sourceOffset = 0;
        if (fromIndex >= sourceCount) {
            return (targetCount == 0 ? sourceCount : -1);
        }
        if (fromIndex < 0) {
            fromIndex = 0;
        }
        if (targetCount == 0) {
            return fromIndex;
        }

        char first = text.charAt(targetOffset);
        int max = sourceOffset + (sourceCount - targetCount);

        for (int i = sourceOffset + fromIndex; i <= max; i++) {
            /* Look for first character. */
            if (charAt(i) != first) {
                while (++i <= max && charAt(i) != first) ;
            }

            /* Found first character, now look at the rest of v2 */
            if (i <= max) {
                int j = i + 1;
                int end = j + targetCount - 1;
                for (int k = targetOffset + 1; j < end && charAt(j) == text.charAt(k); j++, k++) ;

                if (j == end) {
                    /* Found whole string. */
                    return i - sourceOffset;
                }
            }
        }
        return -1;
    }

    default Utf8 trim() {
        int len = length();
        int st = 0;

        while ((st < len) && (charAt(st) <= ' ')) {
            st++;
        }
        while ((st < len) && (charAt(len - 1) <= ' ')) {
            len--;
        }
        return ((st > 0) || (len < length())) ? subSequence(st, len) : this;
    }

    byte[] bytes();

    /**
     * Strip all whitespace characters in the text
     *
     * @return Text with all whitespace removed
     */
    Utf8 removeWhitespace();

    class Wrapper implements Utf8 {

        private final String target;

        private Wrapper(final String target) {
            this.target = target;
        }

        @Override
        public void close() {}

        @Override
        public int length() {
            return target.length();
        }

        @Override
        public char charAt(int index) {
            return target.charAt(index);
        }

        @Override
        public Utf8 subSequence(int start, int end) {
            return Utf8.of(target.substring(start, end));
        }

        @Override
        public int indexOf(String text, int offset) {
            return target.indexOf(text, offset);
        }

        @Override
        public Utf8 trim() {
            return Utf8.of(target.trim());
        }

        @Override
        public byte[] bytes() {
            return Text.bytes(target);
        }

        @Override
        public Utf8 removeWhitespace() {
            return Utf8.of(target.replaceAll("\\s+", ""));
        }
    }

    /** Byte array backed Utf8 */
    class Chars implements Utf8 {
        private final transient CharBuffer text;

        private Chars(final CharBuffer text) {
            this.text = text;
        }

        @Override
        public void close() {
            Eraser.erase(text.array());
        }

        @Override
        public int length() {
            return text.length();
        }

        @Override
        public char charAt(int index) {
            final char c = text.charAt(index);
            return c;
        }

        @Override
        public Utf8 subSequence(int beginIndex, int endIndex) {
            if (beginIndex == 0 && endIndex == text.length()) {
                return this;
            } else {
                final CharBuffer subSequence = text.subSequence(beginIndex, endIndex);
                return new Chars(copyOf(subSequence));
            }
        }

        private CharBuffer copyOf(CharBuffer existing) {
            final CharBuffer copy = CharBuffer.allocate(existing.remaining());
            final CharBuffer temp = existing.asReadOnlyBuffer();
            copy.put(temp);
            copy.flip();
            return copy;
        }

        public byte[] bytes() {
            return Text.bytes(text);
        }

        @Override
        public Utf8 removeWhitespace() {
            final CharBuffer modified = CharBuffer.allocate(text.remaining());

            for (int i = 0; i < text.remaining(); ++i) {
                final char c = text.charAt(i);
                if (c > ' ') {
                    modified.put(c);
                }
            }
            modified.flip();
            return new Chars(modified);
        }

        @Override
        public String toString() {
            return text.toString();
        }
    }
}
