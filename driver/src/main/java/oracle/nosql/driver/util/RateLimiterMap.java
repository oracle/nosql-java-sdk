/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.util;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import oracle.nosql.driver.RateLimiter;

/**
 * A map of table names to RateLimiter instances.
 *
 * Each entry in the map has both a read and write rate limiter instance.
 *
 */

public class RateLimiterMap {

    public class Entry {
        RateLimiter readLimiter;
        RateLimiter writeLimiter;

        private Entry(RateLimiter readLimiter, RateLimiter writeLimiter) {
            this.readLimiter = readLimiter;
            this.writeLimiter = writeLimiter;
        }
    }

    /*
     * Map of RateLimiters, keyed by tableName (or OCID)
     */
    private final Map<String, Entry> limiterMap;


    /**
     * Creates a rate limiter map.
     *
     */
    public RateLimiterMap() {
        limiterMap = new ConcurrentHashMap<String, Entry>();
    }

    /**
     * put a new Entry into the map if it does not exist.
     *
     * If the specified rate limiter already exists, its units will be updated
     *
     * @param tableName name or OCID of the table
     * @param readUnits number of read units per second
     * @param writeUnits number of write units per second
     * @param durationSecs duration in seconds
     */
    public synchronized void update(
        String tableName, double readUnits, double writeUnits,
        double durationSeconds) {

        if (readUnits <= 0.0 && writeUnits <= 0.0) {
            remove(tableName);
            return;
        }

        String lowerTable = tableName.toLowerCase();
        Entry rle = limiterMap.get(lowerTable);
        if (rle == null) {
            RateLimiter rrl = new SimpleRateLimiter(readUnits, durationSeconds);
            RateLimiter wrl = new SimpleRateLimiter(writeUnits, durationSeconds);
            limiterMap.put(lowerTable, new Entry(rrl, wrl));
        } else {
            rle.readLimiter.setLimitPerSecond(readUnits);
            rle.writeLimiter.setLimitPerSecond(writeUnits);
        }
    }

    /**
     * Remove limiters from the map based on table name.
     */
    public void remove(String tableName) {
        limiterMap.remove(tableName.toLowerCase());
    }

    /**
     * get a Read RateLimiter instance from the map.
     *
     * @param tableName name or OCID of the table
     *
     * @return the RateLimiter instance, or null if it does not exist in the map
     */
    public RateLimiter getReadLimiter(String tableName) {
        Entry rle = limiterMap.get(tableName.toLowerCase());
        if (rle == null) {
            return null;
        }
        return rle.readLimiter;
    }

    /**
     * get a Write RateLimiter instance from the map.
     *
     * @param tableName name or OCID of the table
     *
     * @return the RateLimiter instance, or null if it does not exist in the map
     */
    public RateLimiter getWriteLimiter(String tableName) {
        Entry rle = limiterMap.get(tableName.toLowerCase());
        if (rle == null) {
            return null;
        }
        return rle.writeLimiter;
    }

    /**
     * Return true if a pair of limiters exist for given table.
     * This can be used to accelerate non-limited operations (skip if
     * not doing rate limiting).
     */
    public boolean limitersExist(String tableName) {
        return (limiterMap.get(tableName.toLowerCase()) != null);
    }

}
