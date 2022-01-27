/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
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
 * <p>
 * <strong>Logging in the SDK</strong>
 * <p>
 * The SDK uses logging as provided by the <i>java.util.logging</i> package.
 * By default the SDK will log to the console at level INFO. If nothing
 * goes wrong there is little or no logging. There are 2 simple ways to
 * configure logging if additional information is desired.
 * <ol>
 * <li>Create and use a logging configuration file and pass it to the
 * application using a system property. E.g.
 * <pre>
 *     $ java -Djava.util.logging.config.file=path.to.logging.properties ...
 *  or if using mvn:exec
 *     $ mvn exec:java -Djava.util.logging.config.file=path.to.logging.properties ...
 * </pre>
 * The content of the properties file is as documented by
 * <i>java.util.logging</i>. See below for an example. This mechanism makes
 * sense if the application doesn't have its own logging needs.
 * <li>
 * Creating an instance of {@link java.util.logging.Logger} in the application
 * and passing it to the SDK using {@link NoSQLHandleConfig#setLogger}. This
 * option can be used if the application wants to create its own custom
 * logging configuration. It can also use a logging configuration file.
 * </li>
 * </ol>
 * Here is an example of a logging properties file that will log
 * both the SDK and the underlying Netty networking at level FINE. It will
 * log to both the console and a local file called "driver.log."
 * <pre>
handlers=java.util.logging.FileHandler, java.util.logging.ConsoleHandler

# File config
java.util.logging.FileHandler.level=ALL
java.util.logging.FileHandler.formatter=java.util.logging.SimpleFormatter
java.util.logging.FileHandler.pattern=driver.log
java.util.logging.FileHandler.count=1
java.util.logging.FileHandler.limit=50000

# Console config
java.util.logging.ConsoleHandler.level=ALL
java.util.logging.ConsoleHandler.formatter=java.util.logging.SimpleFormatter

# Use a non-default, single-line, simple format pattern
java.util.logging.SimpleFormatter.format=%1$tF %1$tT %4$-7s %5$s %n

# Level can be SEVERE, WARNING, INFO, FINE, ALL, OFF
oracle.nosql.level=FINE
io.netty.level=FINE
 * </pre>
 *
 * <p><strong>Logging internal SDK statistics</strong></p>
 * <p>There are 4 profiles of statistics information that can be logged:
 * <i>none, regular, more, all</i>. For <i>none</i>, which is the default, no
 * statistics  are collected and there are no messages logged.</p>
 *  <p>For <i>regular, more and all</i> profiles, the statistics data is
 *  collected for an interval of time. At the  end  of the
 * interval, the stats data is logged in a specified JSON format that can be
 * filtered and parsed. At the end of each interval, after the logging, the
 * counters are cleared and collection of data resumes.</p><p>
 *
 * Collection intervals are aligned to the top of the hour. This means first
 * interval logs may contain stats for a shorter interval.</p><p>
 *
 * Collection of stats are controlled by the following system
 * properties:<ol><li>
 *   -Dcom.oracle.nosql.sdk.nosqldriver.stats.profile=[none|regular|more|all]
 *      Specifies the stats profile: <i>none</i> - disabled,
 *      <i>regular</i> - per request: counters, errors, latencies, delays, retries
 *      <i>more</i> - stats above with 95th and 99th percentile latencies
 *      <i>all</i> - stats above with per query information.</li><li>
 *
 *   -Dcom.oracle.nosql.sdk.nosqldriver.stats.interval=600 Interval in
 *   seconds to log the stats, by default is 10 minutes.</li><li>
 *
 *   -Dcom.oracle.nosql.sdk.nosqldriver.stats.pretty-print=true Option
 *   to enable pretty printing of the JSON data, default value is false</li>
 *   </ol><p>
 *
 * Statistics can also be enabled by using the API:
 * {@link oracle.nosql.driver.NoSQLHandleConfig#setStatsProfile(StatsControl.Profile)}
 * or {@link oracle.nosql.driver.StatsControl#setProfile(StatsControl.Profile)}.
 * At runtime stats collection can be used selectively by using
 * {@link oracle.nosql.driver.StatsControl#start()} and
 * {@link oracle.nosql.driver.StatsControl#stop()}. The following example shows
 * how to use a stats handler:</p>
 * <pre>
 *     NoSQLHandleConfig config = new NoSQLHandleConfig( endpoint );
 *     config.setStatsProfile(StatsControl.Profile.REGULAR);
 *     config.setStatsInterval(600);
 *     config.setStatsPrettyPrint(false);
 *     config.setStatsHandler(
 *         new StatsControl.StatsHandler() {
 *             public void accept(FieldValue jsonStats) {
 *                 System.out.println("!!! Got a stat: " + jsonStats);
 *             }
 *         });
 *     NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config);
 *
 *     StatsControl statsControl = handle.getStatsControl();
 *
 *     //... application code without stats
 *
 *     // enable observations
 *     statsControl.start();
 *
 *     //... application code with REGULAR stats
 *
 *     // For particular parts of code profile can be changed collect more stats.
 *     statsControl.setProfile(StatsControl.Profile.ALL)
 *     //... more sensitive code with ALL stats
 *     statsControl.setProfile(StatsControl.Profile.REGULAR)
 *
 *     //... application code with REGULAR stats
 *
 *     // disable observations
 *     statsControl.stop();
 *
 *     // ... application code without stats
 *     handle.close();
 * </pre><p>
 *
 *  The following is an example of stats log entry using the ALL
 *  profile:<ol><li>
 *   A one time entry containing stats id and options:
 *    <pre>INFO: Client stats|{    // INFO log entry
 *    "sdkName" : "Oracle NoSQL SDK for Java",  // SDK name
 *    "sdkVersion" : "current",                 // SDK version
 *    "clientId" : "f595b333",                  // NoSQLHandle id
 *    "profile" : "ALL",                        // stats profile
 *    "intervalSec" : 600,                      // interval length in seconds
 *    "prettyPrint" : true,                     // JSON pretty print
 *    "rateLimitingEnabled" : false}            // if rate limiting is
 *    enabled</pre></li><li>
 *   An entry at the end of each interval containing the stats values:
 *    <pre>INFO: Client stats|{
 *   "clientId" : "b7bc7734",              // id of NoSQLHandle object
 *   "startTime" : "2021-09-20T20:11:42Z", // UTC start interval time
 *   "endTime" : "2021-09-20T20:11:47Z",   // UTC end interval time
 *   "requests" : [{                       // array of types of requests
 *     "name" : "Get",                       // stats for GET request type
 *     "httpRequestCount" : 2,               // count of http requests
 *     "errors" : 0,                         // number of errors in interval
 *     "httpRequestLatencyMs" : {            // response time of http requests
 *       "min" : 4,                            // minimum value in interval
 *       "avg" : 4.5,                          // average value in interval
 *       "max" : 5,                            // maximum value in interval
 *       "95th" : 5,                           // 95th percentile value
 *       "99th" : 5                            // 99th percentile value
 *     },
 *     "requestSize" : {                     // http request size in bytes
 *       "min" : 42,                           // minimum value in interval
 *       "avg" : 42.5,                         // average value in interval
 *       "max" : 43                            // maximum value in interval
 *     },
 *     "resultSize" : {                      // http result size in bytes
 *       "min" : 193,                          // minimum value in interval
 *       "avg" : 206.5,                        // average value in interval
 *       "max" : 220                           // maximum value in interval
 *     },
 *     "rateLimitDelayMs" : 0,               // delay in milliseconds introduced by the rate limiter
 *     "retry" : {                           // retries
 *       "delayMs" : 0,                        // delay in milliseconds introduced by retries
 *       "authCount" : 0,                      // no of auth retries
 *       "throttleCount" : 0,                  // no of throttle retries
 *       "count" : 0                           // total number of retries
 *     }
 *   }, {
 *     "name" : "Query",                   // stats for all QUERY type requests
 *     "httpRequestCount" : 14,
 *     "errors" : 0,
 *     "httpRequestLatencyMs" : {
 *       "min" : 3,
 *       "avg" : 13.0,
 *       "max" : 32,
 *       "95th" : 32,
 *       "99th" : 32
 *     },
 *     "resultSize" : {
 *       "min" : 146,
 *       "avg" : 7379.71,
 *       "max" : 10989
 *     },
 *     "requestSize" : {
 *       "min" : 65,
 *       "avg" : 709.85,
 *       "max" : 799
 *     },
 *     "rateLimitDelayMs" : 0,
 *     "retry" : {
 *       "delayMs" : 0,
 *       "authCount" : 0,
 *       "throttleCount" : 0,
 *       "count" : 0
 *     }
 *   }, {
 *     "name" : "Put",                    // stats for PUT type requests
 *     "httpRequestCount" : 1002,
 *     "errors" : 0,
 *     "httpRequestLatencyMs" : {
 *       "min" : 1,
 *       "avg" : 4.41,
 *       "max" : 80,
 *       "95th" : 8,
 *       "99th" : 20
 *     },
 *     "requestSize" : {
 *       "min" : 90,
 *       "avg" : 90.16,
 *       "max" : 187
 *     },
 *     "resultSize" : {
 *       "min" : 58,
 *       "avg" : 58.0,
 *       "max" : 58
 *     },
 *     "rateLimitDelayMs" : 0,
 *     "retry" : {
 *       "delayMs" : 0,
 *       "authCount" : 0,
 *       "throttleCount" : 0,
 *       "count" : 0
 *     }
 *   }],
 *   "queries" : [{            // query stats aggregated by query statement
 *                               // query statement
 *     "query" : "SELECT * FROM audienceData ORDER BY cookie_id",
 *                               // query plan description
 *     "plan" : "SFW([6])\n[\n  FROM:\n  RECV([3])\n  [\n    DistributionKind : ALL_PARTITIONS,\n    Sort Fields : sort_gen,\n\n  ] as $from-0\n\n  SELECT:\n  FIELD_STEP([6])\n  [\n    VAR_REF($from-0)([3]),\n    audienceData\n  ]\n]",
 *     "doesWrites" : false,
 *     "httpRequestCount" : 12,  // number of http calls to the server
 *     "unprepared" : 1,         // number of query requests without prepare
 *     "simple" : false,         // type of query
 *     "countAPI" : 20,          // number of handle.query() API calls
 *     "errors" : 0,             // number of calls trowing exception
 *     "httpRequestLatencyMs" : {// response time of http requests in milliseconds
 *       "min" : 8,                // minimum value in interval
 *       "avg" : 14.58,            // average value in interval
 *       "max" : 32,               // maximum value in interval
 *       "95th" : 32,              // 95th percentile value in interval
 *       "99th" : 32               // 99th percentile value in interval
 *     },
 *     "requestSize" : {         // http request size in bytes
 *       "min" : 65,               // minimum value in interval
 *       "avg" : 732.5,            // average value in interval
 *       "max" : 799               // maximum value in interval
 *     },
 *     "resultSize" : {          // http result size in bytes
 *       "min" : 914,              // minimum value in interval
 *       "avg" : 8585.33,          // average value in interval
 *       "max" : 10989             // maximum value in interval
 *     },
 *     "rateLimitDelayMs" : 0,   // total delay introduced by rate limiter in milliseconds
 *     "retry" : {               // automatic retries
 *       "delayMs" : 0,            // delay introduced by retries
 *       "authCount" : 0,          // count of auth related retries
 *       "throttleCount" : 0,      // count of throttle related retries
 *       "count" : 0               // total count of retries
 *     }
 *   }],
 *   "connections" : {           // concurrent opened connections
 *     "min" : 1,                  // minimum value in interval
 *     "avg" : 9.58,               // average value in interval
 *     "max" : 10                  // maximum value in interval
 *   }
 * }
 *     </pre></li></ol>
 */
package oracle.nosql.driver;
