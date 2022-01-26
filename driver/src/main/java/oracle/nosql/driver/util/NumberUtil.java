/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

import java.math.BigDecimal;
import java.math.BigInteger;

/**
 * This class encapsulates methods to serialize/deserialize BigDecimal value
 * to/from a byte array.
 *
 * The code are directly from KV's oracle.kv.impl.api.table.NumberUtils.
 */
public class NumberUtil {

    /* The terminator byte */
    private static final byte TERMINATOR = (byte)-1;

    /* The leading byte of ZERO */
    private static final byte ZERO = excess128(0);

    /* The byte array that represents ZERO */
    private static final byte[] BYTES_ZERO = new byte[]{ ZERO };

    /**
     * Serializes a BigDecimal value to a byte array that supports byte-to-byte
     * comparison.
     *
     * First, we need to do the normalization, which means we normalize a
     * given BigDecimal into two parts: exponent and mantissa.
     *
     * The decimal part contains 1 integer(non zero). For example,
     *      1234.56 will be normalized to 1.23456E3;
     *      123.4E100 will be normalized to 1.234E102;
     *      -1234.56E-100 will be normalized to -1.23456E-97.
     *
     * The byte format is:
     *     Byte 0 ~ m: the leading bytes that represents the combination of sign
     *                 and exponent, m is from 1 to 5.
     *     Byte m ~ n: the mantissa part with sign, each byte represents every
     *                 2 digits, a terminator byte (-1) is appended at the end.
     *
     * @param value the value to serialize
     * @return the serialized byte array
     */
    public static byte[] serialize(BigDecimal value) {
        if (value.compareTo(BigDecimal.ZERO) == 0) {
            return BYTES_ZERO;
        }

        BigDecimal decimal = value.stripTrailingZeros();
        int sign = decimal.signum();
        String mantissa = decimal.unscaledValue().abs().toString();
        int exponent = mantissa.length() - 1 - decimal.scale();
        return writeBytes(sign, exponent, mantissa);
    }

    /**
     * Deserializes to a BigDecimal object from a byte array.
     * @param bytes the serialized value
     * @return the deserialized value
     */
    public static BigDecimal deserialize(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            throw new IllegalStateException("Leading bytes should be null " +
                    "or empty");
        }

        int offset = 0;
        byte byte0 = bytes[offset++];
        if (!isValidLeadingByte(byte0)) {
            throw new IllegalStateException("Invalid leading byte: " + byte0);
        }

        int sign = readSign(byte0);
        if (sign == 0) {
            return BigDecimal.ZERO;
        }
        ReadBuffer in = new ReadBuffer(bytes, offset);
        int exponent = readExponent(in, byte0, sign);
        return createNumericObject(in, sign, exponent);
    }

    /**
     * Serializes the exponent and mantissa to a byte array and return it
     */
    private static byte[] writeBytes(int sign, int exponent, String digits) {
        return writeBytes(sign, exponent, digits.toCharArray(),
                          0, digits.length());
    }

    private static byte[] writeBytes(int sign, int exponent,
                                     char[] digits, int from, int len) {

        int nLeadingBytes = getNumBytesExponent(sign, exponent);
        /*
         * Required byte # =
         *  leading byte # + mantissa byte # + 1 terminator byte
         */
        int size = nLeadingBytes + (len + 1) / 2 + 1;
        byte[] bytes = new byte[size];
        writeExponent(bytes, 0, sign, exponent);
        writeMantissa(bytes, nLeadingBytes, sign, digits, from, len);
        return bytes;
    }

    /**
     * Write the leading bytes that combines the sign and exponent to
     * the given buffer, the leading bytes format as below:
     *
     *  ------------- ------- ---------------------------------------------
     *  Leading bytes  Sign                  Exponent
     *  ------------- ------- ---------------------------------------------
     *  0xFF                  infinity
     *  0xFC - 0xFE    &gt; 0 unused/reserved
     *  0xFB           &gt; 0 reserved for up to Long.MAX_VALUE
     *  0xF7 - 0xFA    &gt; 0 exponent from 4097 up to Integer.MAX_VALUE,
     *                        1 ~ 5 exponent bytes
     *  0xE7 - 0xF6    &gt; 0 exponent from 64 up to 4096, 1 exponent byte
     *  0x97 - 0xE6    &gt; 0 exponent from -16 up to 63, 0 exponent bytes
     *  0x87 - 0x96    &gt; 0 exponent from -4096 up to -17, 1 exponent byte
     *  0x83 - 0x86    &gt; 0 exponent from Integer.MIN_VALUE up to -4097,
     *                        1 ~ 5 exponent bytes
     *  0x82           &gt; 0 reserved for down to Long.MIN_VALUE
     *  0x81           &gt; 0 reserved for later expansion
     *  0x80           = 0
     *  0x7F           &lt; 0 reserved for later expansion
     *  0x7E           &lt; 0 reserved for down to Long.MIN_VALUE
     *  0x7A - 0x7D    &lt; 0 exponent from -4097 down to Integer.MIN_VALUE,
     *                        1 ~ 5 exponent bytes
     *  0x6A - 0x79    &lt; 0 exponent from -17 down to -4096, 1 exponent byte
     *  0x1A - 0x69    &lt; 0 exponent from 63 down to -16, 0 exponent bytes
     *  0x0A - 0x19    &lt; 0 exponent from 4096 down to 64, 1 exponent byte
     *  0x06 - 0x09    &lt; 0 exponent from Integer.MAX_VALUE down to 4097,
     *                        1 ~ 5 exponent bytes
     *  0x05           &lt; 0 reserved for up to Long.MAX_VALUE
     *  0x01 - 0x04    &lt; 0 unused/reserved
     *  0x00                  -infinity
     */
    private static void writeExponent(byte[] buffer, int offset,
                                      int sign, int value) {

        if (sign == 0) {
            buffer[offset] = (byte)0x80;
        } else if (sign > 0) {
            if (value >= 4097) {
                writeExponentMultiBytes(buffer, offset, (byte)0xF7,
                                        (value - 4097));
            } else if (value >= 64) {
                writeExponentTwoBytes(buffer, offset,
                                      (byte)0xE7, (value - 64));
            } else if (value >= -16) {
                buffer[offset] = (byte)(0x97 + (value - (-16)));
            } else if (value >= -4096) {
                writeExponentTwoBytes(buffer, offset, (byte)0x87,
                                      (value - (-4096)));
            } else {
                writeExponentMultiBytes(buffer, offset, (byte)0x83,
                                        (value - Integer.MIN_VALUE));
            }
        } else {
            /* Sign < 0 */
            if (value <= -4097) {
                writeExponentMultiBytes(buffer, offset, (byte)0x7A,
                                        (-4097 - value));
            } else if (value <= -17){
                writeExponentTwoBytes(buffer, offset, (byte)0x6A,
                                      (-17 - value));
            } else if (value <= 63) {
                buffer[offset] = (byte)(0x1A + (63 - value));
            } else if (value <= 4096) {
                writeExponentTwoBytes(buffer, offset, (byte)0x0A,
                                      (4096 - value));
            } else {
                writeExponentMultiBytes(buffer, offset, (byte)0x06,
                                        (Integer.MAX_VALUE - value));
            }
        }
    }

    /**
     * Writes the leading bytes that represents the exponent with 2 bytes to
     * buffer.
     */
    private static void writeExponentTwoBytes(byte[] buffer,
                                              int offset,
                                              byte byte0,
                                              int exponent) {
        buffer[offset++] = (byte)(byte0 + (exponent >> 8 & 0xF));
        buffer[offset] = (byte)(exponent & 0xFF);
    }

    /**
     * Writes the leading bytes that represents the exponent with variable
     * length bytes to the buffer.
     */
    private static void writeExponentMultiBytes(byte[] buffer,
                                                int offset,
                                                byte byte0,
                                                int exponent) {

        int size = getNumBytesExponentVarLen(exponent);
        buffer[offset++] = (byte)(byte0 + (size - 2));
        if (size > 4) {
            buffer[offset++] = (byte)(exponent >>> 24);
        }
        if (size > 3) {
            buffer[offset++] = (byte)(exponent >>> 16);
        }
        if (size > 2) {
            buffer[offset++] = (byte)(exponent >>> 8);
        }
        buffer[offset] = (byte)exponent;
    }

    /**
     * Returns the number of bytes used to store the exponent value.
     */
    private static int getNumBytesExponent(int sign, int value) {
        if (sign == 0) {
            return 1;
        } else if (sign > 0) {
            if (value >= 4097) {
                return getNumBytesExponentVarLen(value - 4097);
            } else if (value >= 64) {
                return 2;
            } else if (value >= -16) {
                return 1;
            } else if (value >= -4096) {
                return 2;
            } else {
                return getNumBytesExponentVarLen(value - Integer.MIN_VALUE);
            }
        } else {
            if (value <= -4097) {
                return getNumBytesExponentVarLen(-4097 - value);
            } else if (value <= -17){
                return 2;
            } else if (value <= 63) {
                return 1;
            } else if (value <= 4096) {
                return 2;
            } else {
                return getNumBytesExponentVarLen(Integer.MAX_VALUE - value);
            }
        }
    }

    /**
     * Returns the number bytes that used to store the exponent value with
     * variable length.
     */
    private static int getNumBytesExponentVarLen(int exponent) {
        if ((exponent & 0xFF000000) != 0) {
            return 5;
        } else if ((exponent & 0xFF0000) != 0) {
            return 4;
        } else if ((exponent & 0xFF00) != 0) {
            return 3;
        } else {
            return 2;
        }
    }

    /**
     * Write mantissa digits to the given buffer.
     *
     * The digits are stored as a byte array with a terminator byte (-1):
     * - All digits are stored with sign, 1 byte for every 2 digits.
     * - Pad a zero if the number of digits is odd.
     * - The byte is represented in excess-128.
     * - The terminator used is -1. For negative value, subtract additional 2
     *   to leave -1 as a special byte.
     */
    private static void writeMantissa(byte[] buffer, int offset, int sign,
                                      char[] digits, int start, int len) {
        for (int ind = 0; ind < len / 2; ind++) {
            writeByte(buffer, offset++, sign, digits, start + ind * 2);
        }

        /* Pad a zero if left 1 digit only. */
        if (len % 2 == 1) {
            int last = start + len - 1;
            writeByte(buffer, offset++, sign, new char[]{digits[last], '0'}, 0);
        }

        /* Writes terminator byte. */
        buffer[offset] = excess128(TERMINATOR);
    }

    /**
     * Parses 2 digits to a byte and write to the buffer on the given position.
     */
    private static void writeByte(byte[] buffer, int offset, int sign,
                                  char[] digits, int index) {

        if (digits.length <= index + 1) {
            throw new IllegalArgumentException(
                "Invalid digits buffer with length  " + digits.length +
                ", it should be greater than " + (index + 1));
        }
        int value = (digits[index] - '0') * 10 + (digits[index + 1] - '0');
        if (sign < 0) {
            value = -1 * value;
        }
        buffer[offset] = toUnsignedByte(value);
    }

    /**
     * Converts the value with sign to a unsigned byte. If the value is
     * negative, subtract 2.
     */
    private static byte toUnsignedByte(int value) {
        if (value < 0) {
            value -= 2;
        }
        return excess128(value);
    }

    /**
     * Reads and returns the exponent value from the given TupleInput.
     */
    private static int readExponent(ReadBuffer in, byte byte0, int sign) {

        if (sign == 0) {
            return 0;
        }

        if (sign > 0) {
            if (byte0 >= (byte)0xF7) {
                return readExponentMultiBytes(in, byte0, (byte)0xF7) + 4097;
            } else if (byte0 >= (byte)0xE7) {
                return readExponentTwoBytes(in, byte0, (byte)0xE7) + 64;
            } else if (byte0 >= (byte)0x97) {
                return byte0 - (byte)0x97 + (-16);
            } else if (byte0 >= (byte)0x87) {
                return readExponentTwoBytes(in, byte0, (byte)0x87) + (-4096);
            } else {
                return readExponentMultiBytes(in, byte0, (byte)0x83) +
                       Integer.MIN_VALUE;
            }
        }

        /* Sign < 0 */
        if (byte0 >= (byte)0x7A) {
            return -4097 - readExponentMultiBytes(in, byte0, (byte)0x7A);
        } else if (byte0 >= (byte)0x6A) {
            return -17 - readExponentTwoBytes(in, byte0, (byte)0x6A);
        } else if (byte0 >= (byte)0x1A) {
            return 63 - (byte0 - (byte)0x1A);
        } else if (byte0 >= (byte)0x0A) {
            return 4096 - readExponentTwoBytes(in, byte0, (byte)0x0A);
        } else {
            return Integer.MAX_VALUE -
                    readExponentMultiBytes(in, byte0, (byte)0x06);
        }
    }

    /**
     * Returns true if the leading byte is valid, false for unused.
     */
    private static boolean isValidLeadingByte(byte byte0) {
        return (byte0 >= (byte)0x83 && byte0 <= (byte)0xFA) ||
               (byte0 >= (byte)0x06 && byte0 <= (byte)0x7D) ||
               byte0 == (byte)0x80 || byte0 == (byte)0x00 ||
               byte0 == (byte)0xFF;
    }

    /**
     * Reads the exponent value represented with 2 bytes from the given
     * TupleInput and returns it
     */
    private static int readExponentTwoBytes(ReadBuffer in,
                                            byte byte0,
                                            byte base) {
        return (byte0 - base) << 8 | (in.read() & 0xFF);
    }

    /**
     * Reads the exponent value represented with multiple bytes from the
     * given TupleInput and returns it
     */
    private static int readExponentMultiBytes(ReadBuffer in,
                                              byte byte0,
                                              byte base){
        int len = (byte0 - base) + 1;
        int exp = 0;
        while (len-- > 0) {
            exp = exp << 8 | (in.read() & 0xFF);
        }
        return exp;
    }


    /**
     * Parses the digits string and constructs a numeric object, the numeric
     * object can be any of Integer, Long or BigDecimal according to the value.
     */
    private static BigDecimal createNumericObject(ReadBuffer in,
                                                  int sign,
                                                  int expo) {

        int size = (in.available() - 1) * 2;
        char[] buf = new char[size];
        int ind = 0, val;

        /* Read from byte array and store them into char array*/
        while((val = excess128(in.read())) != TERMINATOR) {
            if (val < 0) {
                val = val + 2;
            }
            String group = Integer.toString(Math.abs(val));
            if (group.length() == 1) {
                buf[ind++] = '0';
                buf[ind++] = group.charAt(0);
            } else {
                buf[ind++] = group.charAt(0);
                buf[ind++] = group.charAt(1);
            }
        }

        /* Remove the tailing zero in mantissa */
        if (buf[ind - 1] == '0') {
            ind--;
        }
        String digits = new String(buf, 0, ind);

        /* Construct BigDecimal value */
        BigInteger unscaled = new BigInteger(digits);
        if (sign < 0) {
            unscaled = unscaled.negate();
        }
        return new BigDecimal(unscaled, digits.length() - 1)
                .scaleByPowerOfTen(expo);
    }

    /**
     * Returns the sign number that the specified byte stand for.
     */
    private static int readSign(byte byte0) {
        return (byte0 == (byte)0x80) ? 0 : (((byte0 & 0x80) > 0) ? 1 : -1);
    }

    /**
     * Returns the excess128 representation of a byte.
     */
    private static byte excess128(int b) {
        return (byte)(b ^ 0x80);
    }

    /**
     * A simple read byte buffer.
     */
    private static class ReadBuffer {
        private final byte[] bytes;
        private int offset;

        ReadBuffer(byte[] bytes, int offset) {
            this.bytes = bytes;
            this.offset = offset;
        }

        /**
         * Reads the byte at this buffer's current position, and then
         * increments the position. Return -1 if the position is out of range
         * of the buffer.
         */
        byte read() {
            if (offset < bytes.length) {
                return bytes[offset++];
            }
            return -1;
        }

        /**
         * Returns the number of remaining bytes that can be read from this
         * buffer.
         */
        int available() {
            return bytes.length - offset;
        }
    }
}
