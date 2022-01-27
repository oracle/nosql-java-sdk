/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

/**
 * Consistency is used to provide consistency guarantees for read operations.
 * <p>
 * {@link #ABSOLUTE} consistency may be specified to guarantee that current
 * values are read. {@link #EVENTUAL} consistency means that the values read
 * may be very slightly out of date. {@link #ABSOLUTE} consistency results in
 * higher cost, consuming twice the number of read units for the same data
 * relative to {@link #EVENTUAL} consistency, and should only be used when
 * required.
 * </p>
 * <p>
 * It is possible to set a default Consistency for a {@link NoSQLHandle} instance
 * by using {@link NoSQLHandleConfig#setConsistency}. If no Consistency
 * is specified in an operation and there is no default value, {@link #EVENTUAL}
 * is used.
 * </p>
 * <p>
 * Consistency can be specified as an optional argument to all read operations.
 * </p>
 */
public enum Consistency {
    EVENTUAL,
    ABSOLUTE
}
