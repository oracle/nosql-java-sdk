/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * An exception that is thrown when a table operation fails because the
 * table is in use or busy. Only one modification operation at a time is
 * allowed on a table.
 *
 * @deprecated this class is no longer used
 */
@Deprecated
public class TableBusyException extends RetryableException {

    private static final long serialVersionUID = 1L;

    public TableBusyException(String msg) {
        super(msg);
    }
}
