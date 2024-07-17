/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.http;

import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.StatsControl;
import oracle.nosql.driver.httpclient.HttpClient;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.Request;

public class StatsControlImpl
    implements StatsControl {

    private StatsControl.Profile profile;
    private int interval;
    private boolean prettyPrint;

    private Logger logger;
    private HttpClient httpClient;    /* required for connections */
    private String id = Integer.toHexString(UUID.randomUUID().hashCode());
    private StatsHandler statsHandler;
    private boolean enableCollection = false;
    private Stats stats;

    StatsControlImpl(NoSQLHandleConfig config, Logger logger,
        HttpClient httpClient, boolean rateLimitingEnabled) {
        this.logger = logger;
        this.httpClient = httpClient;

        this.interval = config.getStatsInterval();
        this.profile = config.getStatsProfile();
        this.prettyPrint = config.getStatsPrettyPrint();
        this.statsHandler = config.getStatsHandler();

        if (profile != Profile.NONE) {
            /* when stats collection is enabled set log level to INFO if it
             * is not set.
             */
            if (config.getStatsEnableLog() && !logger.isLoggable(Level.INFO)) {
                logger.setLevel(Level.INFO);
            }
            logger.log(Level.INFO, LOG_PREFIX +
                "{\"sdkName\" : \"Oracle NoSQL SDK for Java" +
                "\", \"sdkVersion\" : \"" +
                NoSQLHandleConfig.getLibraryVersion() +
                "\", \"clientId\" : \"" + id +
                "\", \"profile\" : \"" + profile +
                "\", \"intervalSec\" : " + interval +
                ", \"prettyPrint\" : " + prettyPrint +
                ", \"rateLimitingEnabled\" : " + rateLimitingEnabled + "}");

            start();
        }
    }

    public Logger getLogger() {
        return logger;
    }

    @Override
    public int getInterval() {
        return interval;
    }

    @Override
    public StatsControl setProfile(Profile profile) {
        this.profile = profile;
        return this;
    }

    @Override
    public Profile getProfile() {
        return profile;
    }

    @Override
    public StatsControl setPrettyPrint(boolean enablePrettyPrint) {
        this.prettyPrint = enablePrettyPrint;
        return this;
    }

    @Override
    public boolean getPrettyPrint() {
        return prettyPrint;
    }

    @Override
    public StatsControl setStatsHandler(StatsHandler statsHandler) {
        this.statsHandler = statsHandler;
        return this;
    }

    @Override
    public StatsHandler getStatsHandler() {
        return statsHandler;
    }

    @Override
    public void start() {
        if (profile != Profile.NONE) {
            if (stats == null) {
                stats = new Stats(this);
            }
        }
        enableCollection = true;
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

    void observe(Request kvRequest, int networkLatency,
        int reqSize, int resSize) {
        if (stats != null && enableCollection) {
            stats.observe(kvRequest, false,
                httpClient.getAcquiredChannelCount(),
                reqSize, resSize, networkLatency);
        }
    }

    void observeError(Request kvRequest) {
        if (stats != null && enableCollection) {
            stats.observeError(kvRequest,
                httpClient.getAcquiredChannelCount());
        }
    }

    public void observeQuery(QueryRequest qreq) {
        if (stats != null && enableCollection) {
            stats.observeQuery(qreq);
        }
    }
}
