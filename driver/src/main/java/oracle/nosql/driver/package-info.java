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
 * <p>
 * <strong>Configuration and Multi-threaded Applications</strong>
 * <p>
 * High performance multi-threaded applications may benefit from using
 * some non-default configuration options related to how threads and
 * connections are managed by the networking implementation (Netty).
 * <p>
 * There are 3 methods on {@link NoSQLHandleConfig} that relate to threads
 * and connections. If your application isn't getting the performance
 * expected these can be tuned. There is no single answer to what is best.
 * It is best to experiment with different values and observe the behavior.
 * <ol>
 * <li> {@link NoSQLHandleConfig#setNumThreads}. This is the number
 * threads that the implementation uses to handle connections. By default it
 * is set to the number of available CPUs * 2. This is the Netty default.
 * Unless your application has high latency operations this should be
 * sufficient.
 *  </li>
 * <li> {@link NoSQLHandleConfig#setConnectionPoolSize}. This is the
 * default size of the Netty connection pool. It also defaults to the
 * number of available CPUs * 2. Again, unless you have high latency
 * operations (long queries) this should be sufficient for most needs. </li>
 * <li> {@link NoSQLHandleConfig#setPoolMaxPending}. This parameter is
 * used by Netty to control the number of threads waiting for a connections
 * from the pool. If your applications encounter exceptions of the nature,
 * "too many outstanding acquires" this is the cause. The solution is
 * usually to increase the size of the connection pool as this error means
 * that your application has significantly more threads attempting to send
 * requests than there are connections available.</li>
 * </ol>
 * When tuning multi-threaded applications it's important to remember that
 * it's possible to have too many threads and that more threads does not
 * equal more performance. The optimal number depends on request latency and
 * other I/O performed. It is best to experiment.
 */
package oracle.nosql.driver;
