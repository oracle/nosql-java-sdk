package oracle.nosql.driver.http;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.nosql.driver.ReadThrottlingException;
import oracle.nosql.driver.SecurityInfoNotReadyException;
import oracle.nosql.driver.StatsConfig;
import oracle.nosql.driver.ThrottlingException;
import oracle.nosql.driver.WriteThrottlingException;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.kv.AuthenticationException;
import oracle.nosql.driver.ops.Request;
import oracle.nosql.driver.ops.RetryStats;

public class StatsConfigImpl
    implements StatsConfig {

    private final static String PROFILE_PROPERTY =
        "com.oracle.nosql.sdk.nosqldriver.stats.profile";
    private final static String INTERVAL_PROPERTY =
        "com.oracle.nosql.sdk.nosqldriver.stats.interval";
    private final static String PRETTY_PRINT_PROPERTY =
        "com.oracle.nosql.sdk.nosqldriver.stats.pretty-print";
    final static String LOG_PREFIX = "ONJS:Monitoring stats|";

    private StatsConfig.Profile profile = Profile.NONE;
    /* Time interval to log in seconds. Default 600, ie. 10 minutes. */
    private int interval = 600;
    private boolean prettyPrint = false;

    private Logger logger;
    private HttpClient httpClient;    /* required for connections */
    private String id = Integer.toHexString(UUID.randomUUID().hashCode());
    private StatsHandler statsHandler;
    private boolean enableCollection = false;
    private Stats stats;

    StatsConfigImpl(String libraryVersion, Logger logger,
        HttpClient httpClient, boolean rateLimitingEnabled) {
        this.logger = logger;
        this.httpClient = httpClient;

        String profileProp = System.getProperty(PROFILE_PROPERTY);
        if (profileProp != null) {
            try {
                setProfile(Profile.valueOf(profileProp.toUpperCase()));
            } catch (IllegalArgumentException iae) {
                logger.log(Level.SEVERE, LOG_PREFIX  + "Invalid profile " +
                    "value for system property " + PROFILE_PROPERTY + ": " +
                    profileProp);
            }
        }

        String intervalProp = System.getProperty(INTERVAL_PROPERTY);
        if (intervalProp != null) {
            try {
                setInterval(Integer.valueOf(intervalProp));
            } catch (NumberFormatException nfe) {
                logger.log(Level.SEVERE, "Invalid integer value for system " +
                    "property " + INTERVAL_PROPERTY + ": " + intervalProp);
            }
        }

        String ppProp = System.getProperty(PRETTY_PRINT_PROPERTY);
        if (ppProp != null && ("true".equals(ppProp.toLowerCase()) || "1".equals(ppProp) ||
            "on".equals(ppProp.toLowerCase()))) {
            prettyPrint = Boolean.valueOf(ppProp);
        }

        if (profile != Profile.NONE) {
            logger.setLevel(Level.INFO);
            logger.log(Level.INFO, LOG_PREFIX +
                "{\"sdkName\"=\"Oracle NoSQL SDK for Java\", " +
                "\"sdkVersion\":\"" + libraryVersion + "\", " +
                "clientId=\"" + id + "\",\"profile\":\"" + profile + "\", " +
                "\"intervalSec\"=" + interval +
                ", \"prettyPrint\"=" + prettyPrint +
                ", \"rateLimitingEnabled\"=" + rateLimitingEnabled + "}");

            start();
        }
    }

    @Override
    public StatsConfig setLogger(Logger logger) {
        this.logger = logger;
        return this;
    }

    @Override
    public Logger getLogger() {
        return logger;
    }

    @Override
    public StatsConfig setInterval(int interval) {
        if (interval < 1) {
            throw new IllegalArgumentException("Stats interval can not be " +
                "less than 1 second.");
        }
        this.interval = interval;
        return this;
    }

    @Override
    public int getInterval() {
        return interval;
    }

    @Override
    public StatsConfig setProfile(Profile profile) {
        this.profile = profile;
        return this;
    }

    @Override
    public Profile getProfile() {
        return profile;
    }

    @Override
    public StatsConfig setPrettyPrint(boolean enablePrettyPrint) {
        this.prettyPrint = enablePrettyPrint;
        return this;
    }

    @Override
    public boolean getPrettyPrint() {
        return prettyPrint;
    }

    @Override
    public void registerHandler(StatsHandler statsHandler) {
        this.statsHandler = statsHandler;
    }

    public StatsHandler getHandler() {
        return statsHandler;
    }

    @Override
    public void start() {
        if (profile == Profile.NONE) {
            stats = null;
        } else if (stats == null) {
            stats = new Stats(this);
            enableCollection = true;
        }
    }

    @Override
    public void stop() {
        enableCollection = false;
    }

    @Override
    public boolean isStarted() {
        return enableCollection;
    }

    public String getId() {
        return id;
    }

    public void shutdown() {
        if (stats != null) {
            stats.shutdown();
        }
    }

    void observe(Request kvRequest, int wireTime,
        int reqSize, int resSize) {
        if (stats != null && enableCollection) {
            String requestClass = kvRequest.getClass().getSimpleName();
            int auth = 0, throttle = 0, retries = 0, retryDelay = 0;
            RetryStats retryStats = kvRequest.getRetryStats();
            if (retryStats != null) {

                auth = retryStats.getNumExceptions(
                    AuthenticationException.class);
                auth += retryStats.getNumExceptions(
                    SecurityInfoNotReadyException.class);

                throttle = retryStats.getNumExceptions(
                    ThrottlingException.class);

                retries = retryStats.getRetries();
                retryDelay = retryStats.getDelayMs();
            }

            int rateLimitDelay = kvRequest.getRateLimitDelayedMs();

            stats.observe(requestClass, false, retries, retryDelay,
                rateLimitDelay, auth, throttle,
                httpClient.getAcquiredChannelCount(),
                reqSize, resSize, wireTime);
        }
    }

    void observeError(Request kvRequest) {
        if (stats != null && enableCollection) {
            String requestClass = kvRequest.getClass().getSimpleName();
            int authCount = 0, throttleCount = 0, retryCount = 0, retryDelay = 0;
            RetryStats retryStats = kvRequest.getRetryStats();
            if (retryStats != null) {
                authCount = retryStats.getNumExceptions(
                    AuthenticationException.class);
                authCount += retryStats.getNumExceptions(
                    SecurityInfoNotReadyException.class);

                throttleCount = retryStats.getNumExceptions(
                    ThrottlingException.class);
                throttleCount += retryStats.getNumExceptions(
                    ReadThrottlingException.class);
                throttleCount += retryStats.getNumExceptions(
                    WriteThrottlingException.class);

                retryCount = retryStats.getRetries();
                retryDelay = retryStats.getDelayMs();
            }

            int rateLimitDelay = kvRequest.getRateLimitDelayedMs();

            stats.observeError(requestClass, retryCount, retryDelay,
                authCount, throttleCount, rateLimitDelay,
                httpClient.getAcquiredChannelCount());
        }
    }
}
