/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.changestream;

import static oracle.nosql.driver.util.TimestampUtil.parseString;

import java.sql.Timestamp;

public class StartLocation {

    public enum LocationType {
        UNINITIALIZED(0),

        /**
         * Start consuming at the first uncommitted message in the stream.
         *
         * This type says "start consumption at the first record that hasn’t
         * been committed". For a new table that has no commits, this
         * will be the first record in the stream - same as EARLIEST.
         *
         * This is the default.
         */
        FIRST_UNCOMMITTED(1),

        /* Start consuming from the earliest (oldest) available message in the stream. */
        EARLIEST(2),

        /* Start consuming messages that were published after the start of the consumer. */
        LATEST(3),

        /* Start consuming from a given time. */
        AT_TIME(4);

        LocationType(int i) {
        }
    }

    public LocationType location;
    public long startTime;

    StartLocation(LocationType location, long startTime) {
        this.location = location;
        this.startTime = startTime;
    }

    public static StartLocation latest() {
        return new StartLocation(LocationType.LATEST, 0);
    }

    public static StartLocation earliest() {
        return new StartLocation(LocationType.EARLIEST, 0);
    }

    public static StartLocation firstUncommitted() {
        return new StartLocation(LocationType.FIRST_UNCOMMITTED, 0);
    }

    public static StartLocation atTime(long startTime) {
        return new StartLocation(LocationType.AT_TIME, startTime);
    }

    public static StartLocation atTime(Timestamp startTime) {
        return new StartLocation(LocationType.AT_TIME, startTime.getTime());
    }

    /**
     * Creates a StartLocation AT_TIME instance using an ISO 8601
     * formatted string.
     *
     * If timezone is not specified it is interpreted as UTC.
     *
     * @param startTime the string of a Timestamp in ISO 8601 format
     * "uuuu-MM-dd['T'HH:mm:ss[.f..f]]".
     */
    public static StartLocation atTime(String startTime) {
        return StartLocation.atTime(startTime, null, true);
    }

    /**
     * Creates a StartLocation AT_TIME instance from a date string with
     * specified pattern.
     *
     * @param startTime a timestamp string in the format of the specified
     * {@code pattern} or the default pattern, "uuuu-MM-dd['T'HH:mm:ss[.f..f]]",
     * which is used if {@code pattern} is null.
     *
     * @param pattern the pattern for the timestampString. If null, then default
     * pattern "uuuu-MM-dd['T'HH:mm:ss[.f..f]]" is used to parse the string.
     * The symbols that can be used to specify a pattern are described in
     * {@link java.time.format.DateTimeFormatter}.
     *
     * @param useUTC if true the UTC time zone is used as default zone when
     * parsing the string, otherwise the local time zone is used.
     * If the timestampString has zone information, then that zone will be used.
     *
     * @throws IllegalArgumentException if the string cannot be parsed with the
     * the given pattern correctly.
     */
    public static StartLocation atTime(String startTime,
                                       String pattern,
                                       boolean useUTC) {
        return new StartLocation(LocationType.AT_TIME,
            parseString(startTime,
                        pattern,
                        useUTC,
                        (pattern == null)).getTime());
    }
}
