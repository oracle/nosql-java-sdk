/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

/**
 * A class to hold query exceptions indicating syntactic or semantic problems
 * at the driver side during query execution. It is internal use only and it
 * will be caught, and rethrown as IllegalArgumentException to the application.
 * It includes location information. When converted to an IAE, the location info
 * is put into the message created for the IAE.
 */
public class QueryException extends RuntimeException {

    static final long serialVersionUID = 1;

    /**
     * Location of an expression in the query. It contains both start and
     * end, line and column info.
     */
    public static class Location {

        private final int startLine;
        /* defined as char position in line */
        private final int startColumn;
        private final int endLine;
        /* defined as char position in line */
        private final int endColumn;

        public Location(
            int startLine,
            int startColumn,
            int endLine,
            int endColumn) {
            this.startLine = startLine;
            this.startColumn = startColumn;
            this.endLine = endLine;
            this.endColumn = endColumn;

            assert(startLine >= 0);
            assert(startColumn >= 0);
            assert(endLine >= 0);
            assert(endColumn >= 0);
        }

        @Override
        public String toString() {
            return startLine + ":" + startColumn + "-" + endLine + ":" +
                endColumn;
        }

        /**
         * Returns the start line.
         */
        public int getStartLine() {
            return startLine;
        }

        /**
         * Returns the start column as its char position in line.
         */
        public int getStartColumn() {
            return startColumn;
        }

        /**
         * Returns the end line.
         */
        public int getEndLine() {
            return endLine;
        }

        /**
         * Returns the end column as its char position in line.
         */
        public int getEndColumn() {
            return endColumn;
        }
    }

    protected final Location location;

    public QueryException(
        String message,
        Throwable cause,
        QueryException.Location location) {
        super(message, cause);
        this.location = location;
    }

    public QueryException(String message, QueryException.Location location) {
        super(message);
        this.location = location;
    }

    public QueryException(Throwable cause, QueryException.Location location) {
        super(cause);
        this.location = location;
    }

    public QueryException(Throwable cause) {
        super(cause);
        location = null;
    }

    public QueryException(String message) {
        super(message);
        location = null;
    }

    /**
     * Returns the location associated with this exception. May be null if not
     * available.
     */
    public QueryException.Location getLocation() {
        return location;
    }

    /**
     * Get this exception as a simple IAE, not wrapped. This is used on the
     * client side.
     */
    public IllegalArgumentException getIllegalArgument() {
        return new IllegalArgumentException(toString());
    }

    @Override
    public String toString() {
        return "Error:" + (location == null ? "" : " at (" + location.startLine +
            ", " + location.startColumn + ")" ) + " " + getMessage();
    }
}
