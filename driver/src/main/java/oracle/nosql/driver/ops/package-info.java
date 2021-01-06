/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
/**
 * Contains the input and response classes used for Oracle NoSQL
 * Database operations. These include classes derived from
 * {@link oracle.nosql.driver.ops.Request} and
 * {@link oracle.nosql.driver.ops.Result}.
 * <p>
 * {@link oracle.nosql.driver.ops.Request} instances are parameter objects that
 * hold required and optional state for a given request type and
 * operation. Some parameters are defaulted based on the
 * {@link oracle.nosql.driver.NoSQLHandleConfig}
 * and can be overridden in a specific request. Examples of these are timeouts
 * and {@link oracle.nosql.driver.Consistency}. Some parameters are required.
 * For example, most requests require a table name for the target table,
 * {@link oracle.nosql.driver.NoSQLHandle#put} requires a value, etc.
 * <p>
 * Validation of parameter state is not performed until the object is used in a
 * request, so illegal state is not immediately detected.
 * <p>
 * {@link oracle.nosql.driver.ops.Result} instances represent return state for
 * operations that succeed. All result instances contain information about
 * throughput used by that operation if available, in the form of
 * <em>getReadUnits</em> and <em>getWriteUnits</em> interfaces on the objects.
 * When used against an on-premise service resource consumption information is
 * not available and will be 0.
 * <p>
 * {@link oracle.nosql.driver.ops.Request} and
 * {@link oracle.nosql.driver.ops.Result} instances are not thread-safe and not
 * intended to be shared. {@link oracle.nosql.driver.ops.Request} can be reused
 * and are not modified when used in operations, but they should not be modified
 * while in use by the system.
 */
package oracle.nosql.driver.ops;
