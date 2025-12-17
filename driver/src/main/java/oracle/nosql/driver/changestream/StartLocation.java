/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.changestream;


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

        // Start consuming from a given time.
        AT_TIME(4);

        LocationType(int i) {
          //TODO Auto-generated constructor stub
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

}
