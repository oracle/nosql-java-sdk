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
 *   -Dcom.oracle.nosql.sdk.nosqldriver.stats.profile=[none|regular|full]
 *      Specifies the stats profile: <i>none</i> - disabled,
 *      <i>regular</i> - all except 95th and 99th percentile and
 *      <i>full</i> - all including 95th and 99th percentile.</li><li>
 *   -Dcom.oracle.nosql.sdk.nosqldriver.stats.interval=600 Interval in
 *   seconds to log the stats, by default is 10 minutes.</li><li>
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
 *             @Override
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
 * </code></p>
 */
public interface StatsConfig {

    /**
     * The following semantics are attached to the Profile:
     *  - NONE: no stats are logged.
     *  - REGULAR: all stats except 95th and 99th percentile latencies.
     *  - FULL: all stats including 95th and 99th percentile latencies.
     */
    enum Profile {
        NONE, REGULAR, FULL;
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
     * -Dcom.oracle.nosql.sdk.nosqldriver.stats.profile= is "regular" or "full".
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
