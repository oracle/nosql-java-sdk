/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandle;

/**
 * Represents the result of a {@link NoSQLHandle#listTables} operation.
 * <p>
 * On a successful operation the table names are available as well as the
 * index of the last returned table. Tables are returned in an array, sorted
 * alphabetically.
 * @see NoSQLHandle#listTables
 */
public class ListTablesResult extends Result {
    private String[] tables;
    private int lastIndexReturned;

    /**
     * Returns the array of table names returned by the operation.
     *
     * @return the table names
     */
    public String[] getTables() {
        return tables;
    }

    /**
     * Returns the index of the last table name returned. This can be provided
     * to {@link ListTablesRequest} to be used as a starting point for listing
     * tables.
     *
     * @return the index
     */
    public int getLastReturnedIndex() {
        return lastIndexReturned;
    }

    /**
     * @hidden
     * @param tables the tables
     * @return this
     */
    public ListTablesResult setTables(String[] tables) {
        this.tables = tables;
        return this;
    }

    /**
     * @hidden
     * @param lastIndexReturned the index
     * @return this
     */
    public ListTablesResult setLastIndexReturned(int lastIndexReturned) {
        this.lastIndexReturned = lastIndexReturned;
        return this;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("[");
        for (int i = 0; i < tables.length; i++) {
            sb.append("\"").append(tables[i]).append("\"");
            if (i < (tables.length - 1)) {
                sb.append(",");
            }
        }
        sb.append("]");
        return sb.toString();
    }
}
