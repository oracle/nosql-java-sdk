package oracle.nosql.driver;

import java.util.function.Consumer;
import java.util.logging.Logger;

import oracle.nosql.driver.values.FieldValue;

/**
 * <p>This interface allows user to setup the collection of driver statistics.</p><p>
 * 
 * The statistics data is collected for an interval of time. At the end of the
 * interval, the stats data is logged in a specified JSON format that can be
 * filtered and parsed. After the logging, the counters are cleared and
 * collection of data resumes.</p><p>
 *
 * Collection intervals are aligned to the top of the hour. This means first
 * interval logs may contain stats for a shorter interval.</p><p>
 *
 * Collection of stats are controlled by the following system properties:<li>
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
 *   to enable pretty printing of the JSON data, default value is false</li></p>
 *
 * Collection of stats can also be used by using the API: <code>
 *     NoSQLHandleConfig config = new NoSQLHandleConfig( endpoint );
 *     NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config);
 *
 *     StatsConfig statsConfig = handle.getStatsConfig();
 *     statsConfig.setProfile(StatsConfig.Profile.REGULAR);
 *     statsConfig.setInterval(600);
 *     statsConfig.setPrettyPrint(false);
 *     statsConfig.registerHandler(
 *         new StatsConfig.StatsHandler() {
 *             public void accept(FieldValue jsonStats) {
 *                 System.out.println("!!! Got a stat: " + jsonStats);
 *             }
 *         });
 *     statsConfig.start();
 *
 *     //... application code
 *
 *     statsConfig.stop();
 *     handle.close();
 * </code></p><p>
 *
 *     The following is an example of stats log entry using the ALL
 *     profile:<code>
 * INFO: ONJS:Monitoring stats|{
 *   "clientId" : "b7bc7734",
 *   "startTime" : "2021-09-20T20:11:42Z",
 *   "endTime" : "2021-09-20T20:11:47Z",
 *   "requests" : [{
 *     "name" : "Get",
 *     "count" : 2,
 *     "errors" : 0,
 *     "networkLatencyMs" : {
 *       "min" : 4,
 *       "avg" : 4.5,
 *       "max" : 5,
 *       "95th" : 5,
 *       "99th" : 5
 *     },
 *     "requestSize" : {
 *       "min" : 42,
 *       "avg" : 42.5,
 *       "max" : 43
 *     },
 *     "resultSize" : {
 *       "min" : 193,
 *       "avg" : 206.5,
 *       "max" : 220
 *     },
 *     "rateLimitDelayMs" : 0,
 *     "retry" : {
 *       "delayMs" : 0,
 *       "authCount" : 0,
 *       "throttleCount" : 0,
 *       "count" : 0
 *     }
 *   }, {
 *     "name" : "Query",
 *     "count" : 14,
 *     "errors" : 0,
 *     "networkLatencyMs" : {
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
 *     "name" : "Put",
 *     "count" : 1002,
 *     "errors" : 0,
 *     "networkLatencyMs" : {
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
 *   "queries" : [{
 *     "stmt" : "SELECT * FROM audienceData ORDER BY cookie_id",
 *     "plan" : "SFW([6])\n[\n  FROM:\n  RECV([3])\n  [\n    DistributionKind : ALL_PARTITIONS,\n    Sort Fields : sort_gen,\n\n  ] as $from-0\n\n  SELECT:\n  FIELD_STEP([6])\n  [\n    VAR_REF($from-0)([3]),\n    audienceData\n  ]\n]",
 *     "doesWrites" : false,
 *     "count" : 12,
 *     "unprepared" : 1,
 *     "simple" : 0,
 *     "countAPI" : 20,
 *     "errors" : 0,
 *     "networkLatencyMs" : {
 *       "min" : 8,
 *       "avg" : 14.58,
 *       "max" : 32,
 *       "95th" : 32,
 *       "99th" : 32
 *     },
 *     "requestSize" : {
 *       "min" : 65,
 *       "avg" : 732.5,
 *       "max" : 799
 *     },
 *     "resultSize" : {
 *       "min" : 914,
 *       "avg" : 8585.33,
 *       "max" : 10989
 *     },
 *     "rateLimitDelayMs" : 0,
 *     "retry" : {
 *       "delayMs" : 0,
 *       "authCount" : 0,
 *       "throttleCount" : 0,
 *       "count" : 0
 *     }
 *   }],
 *   "connections" : {
 *     "min" : 1,
 *     "avg" : 9.58,
 *     "max" : 10
 *   }
 * }
 *     </code></p>
 */
public interface StatsConfig {

    /**
     * The following semantics are attached to the Profile:
     *  - NONE: no stats are logged.
     *  - REGULAR: per request: counters, errors, latencies, delays, retries
     *  - MORE: stats above with 95th and 99th percentile latencies.
     *  - ALL: stats above with per query information
     */
    enum Profile {
        NONE, REGULAR, MORE, ALL;
    }

    /**
     * Handler interface that user can register to get access to stats at
     * the end of the interval.
     */
    interface StatsHandler extends Consumer<FieldValue> {
        /** Stats are encoded in JSON format using the FieldValue API. */
        void accept(FieldValue jsonStats);
    }

    /**
     * Sets the logger to be used.
     */
    StatsConfig setLogger(Logger logger);

    /**
     * Returns the current logger.
     */
    Logger getLogger();

    /**
     * Sets interval size in seconds.
     * Default interval is 600 seconds, i.e. 10 min.
     */
    StatsConfig setInterval(int interval);


    /**
     * Returns the current collection interval.
     * Default interval is 600 seconds, i.e. 10 min.
     */
    int getInterval();

    /**
     * Set the collection profile.
     * Default profile is NONE.
     */
    StatsConfig setProfile(Profile profile);

    /**
     * Returns the collection profile.
     * Default profile is NONE.
     */
    Profile getProfile();

    /**
     * Enable JSON pretty print for easier human reading.
     * Default is disabled.
     */
    StatsConfig setPrettyPrint(boolean enablePrettyPrint);

    /**
     * Returns the current JSON pretty print flag.
     * Default is disabled.
     */
    boolean getPrettyPrint();

    /**
     * Registers a stats handler.
     * @param handler User defined StatsHandler.
     */
    void registerHandler(StatsHandler handler);

    /**
     * Collection of stats is enabled only between start and stop or from the
     * beginning if system property
     * -Dcom.oracle.nosql.sdk.nosqldriver.stats.profile= is not "none".
     */
    void start();

    /**
     * Stops collection of stats.
     */
    void stop();

    /**
     * Returns true if collection of stats is enabled, otherwise returns false.
     */
    boolean isStarted();
}
