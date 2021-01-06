/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
/**
 * Contains the public API for using the Oracle NoSQL Database
 * as well as configuration and common parameter classes used in
 * database operations. This package contains the major configuration
 * and common operational classes and interfaces, as well as exception
 * classes. Value classes used for data are in the
 * <a href="{@docRoot}/oracle/nosql/driver/values/package-summary.html#package.description">
 * values package.
 * </a>
 * Request and result objects used for individual operations are in the
 * <a href="{@docRoot}/oracle/nosql/driver/ops/package-summary.html#package.description">
 * ops package.
 * </a>
 * <p>
 * This and other packages in the system support both users of the Oracle NoSQL
 * Database Cloud Service and the on-premise Oracle NoSQL Database. Some
 * classes, methods, and parameters are specific to each environment. The
 * documentation for affected classes and methods notes whether there are
 * environment-specific considerations. Unless otherwise noted they are
 * applicable to both environments. The differences mostly related to
 * authentication models, encapsulated in {@link AuthorizationProvider}
 * and resource constraints and limits in the Cloud Service that are
 * not present on-premise.
 * <p>
 * The overall flow of a driver application is:
 * <ol>
 * <li>Configure the handle to use, including the endpoint for the server to
 * use. The configuration object, {@link oracle.nosql.driver.NoSQLHandleConfig},
 * has additional configuration options.</li>
 * <li>Use the configuration object to obtain a
 * {@link oracle.nosql.driver.NoSQLHandle} instance. </li>
 * <li>All data operations are methods on
 * {@link oracle.nosql.driver.NoSQLHandle}. They all have the same pattern of:
 * <ul>
 * <li>Create and configure Request instance for the operations</li>
 * <li>Call the appropriate method on {@link oracle.nosql.driver.NoSQLHandle}
 * for the request</li>
 * <li>Process results in the Result object</li>
 * </ul>
 * Errors are thrown as exceptions. Many exceptions specific to the system
 * are instances of {@link oracle.nosql.driver.NoSQLException}, but common
 * Java exceptions such as
 * {@link java.lang.IllegalArgumentException} are thrown directly.
 * </li>
 * </ol>
 * <p>
 * Instances of {@link oracle.nosql.driver.NoSQLHandle} are thread-safe and
 * intended to be shared in a multi-threaded application. They are associated
 * 1:1 with an identity so they cannot be shared by different users. While
 * they are not extremely expensive, they have a connection pool and thread
 * pool and are not intended to be disposable.
 */
package oracle.nosql.driver;
