/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */
package oracle.nosql.driver.util;

import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Cache implementation based on a LRU eviction policy.
 * <p>
 * In addition to the general LRU policy the implementation supports expired
 * entry cleanup when a lookup is performed. If the lifetime of the entry
 * exceeds the configured maximum entry lifetime, the entry will be removed
 * from the cache.
 * <p>
 * If eviction is enabled and an entry lifetime is specified, background
 * clean is launched in a thread that periodically looks for expired entries
 * and removes them.
 *
 * Because this cache supports expiration of entries the value must extend
 * CacheEntry, which adds time information to the value in the cache.
 */
public class LruCache<K, V> {

    /*
     * load factor for LinkedHashMap
     */
    private static final float LOAD_FACTOR = 0.6f;

    /*
     * Multiply this times the entry lifetime and use that as the sleep
     * period for the cleanup thread if created.
     */
    private static final int EVICTION_PERIOD_FACTOR = 10;

    /* The maximum capacity for the cache */
    private final int capacity;

    /* Maximum lifetime for a value entry in ms */
    private volatile int lifetime;

    /* Map of key and value */
    private final LinkedHashMap<K, CacheEntry<V>> cacheMap;

    /* Background expired entry cleanup task */
    private TimeBasedCleanupTask cleanupTask;

    private final ReentrantLock lock = new ReentrantLock();

    /**
     * Constructs an instance of LruCache. There are 2 parameters that control
     * behavior, capacity and lifetime. If both are 0 the cache is an
     * ever-expanding structure and entries never age out. If capacity is 0
     * then entries are only ever removed because they expire. If lifetime is
     * 0 then entries are only ever removed because the cache has reached
     * capacity.
     *
     * @param capacity the capacity of the cache. If 0 then there is no limit
     * to the size and entries are never removed unless they age out.
     *
     * @param lifetimeMs the lifetime of a cache entry, in milliseconds. 0
     * means entries never expire.
     */
    public LruCache(int capacity, int lifetimeMs) {
        this.capacity = capacity;
        this.lifetime = lifetimeMs;
        if (capacity > 0) {
            /*
             * Implement removeEldestEntry to implement the LRU policy when
             * the cache reaches capacity.
             */
            cacheMap =
                new LinkedHashMap<K, CacheEntry<V>>(capacity,
                                                    LOAD_FACTOR,
                                                    true /* access ordered */) {
                private static final long serialVersionUID = 1L;
                @Override
                protected boolean removeEldestEntry(
                    Map.Entry<K, CacheEntry<V>> entry) {
                    return size() > capacity;
                }
            };
        } else {
            cacheMap = new LinkedHashMap<K, CacheEntry<V>>();
        }

        if (lifetime > 0) {
            /* set up a thread to do cleanup occasionally */
            final long evictionPeriodMs = lifetime * EVICTION_PERIOD_FACTOR;
            cleanupTask = new TimeBasedCleanupTask(evictionPeriodMs);
        }
    }

    /**
     * Returns the value associated with given key in the cache, or null if
     * there is no value cached for key.
     *
     * @param key the key to use
     * @return value the value or null if there is no entry for the key
     */
    public V get(K key) {
        lock.lock();
        try {
            CacheEntry<V> entry = cacheMap.get(key);
            if (entry == null) {
                return null;
            }
            if (isExpired(entry)) {
                cacheMap.remove(key);
                return null;
            }
            return entry.getValue();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stores the key value pair in the cache.
     *
     * If the cache contains the value associated with the key the old value is
     * replaced.
     *
     * @param key the key to use
     * @param value the value to use
     *
     * @return the old value if there was one present, otherwise null.
     */
    public V put(K key, V value) {
        lock.lock();
        try {
            CacheEntry<V> entry = cacheMap.put(key, new CacheEntry<V>(value));
            if (entry != null) {
                return entry.getValue();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Removes the cached value for the given key.
     *
     * @param key the key to use
     * @return the previously cached value or null
     */
    public V remove(K key) {
        lock.lock();
        try {
            CacheEntry<V> entry = cacheMap.remove(key);
            if (entry != null) {
                return entry.getValue();
            }
            return null;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the creation time in milliseconds since the Epoch if available
     * for the entry specified by the key. If there is no entry for the key
     * 0 is returned.
     *
     * @param key the key to use
     * @return the creation time, or 0 if no entry is found for the key
     */
    public long getCreationTime(K key) {
        lock.lock();
        try {
            CacheEntry<V> entry = cacheMap.get(key);
            if (entry != null) {
                return entry.getCreateTime();
            }
            return 0L;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the capacity of the cache
     *
     * @return the capacity in number of entries
     */
    public int getCapacity() {
        return this.capacity;
    }

    /**
     * Returns a set of all values in the cache. The values are live from the
     * cache.
     *
     * @return all values.
     */
    public Set<V> getAllValues() {
        lock.lock();
        try {
            final Set<V> copy = new HashSet<V>();
            for (CacheEntry<V> entry : cacheMap.values()) {
                copy.add(entry.getValue());
            }
            return copy;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Stops all cache background tasks. This will only affect a cache with
     * a non-zero lifetime set for entries.
     *
     * @param wait set to true to wait for the background tasks to finish
     */
    public void stop(boolean wait) {
        if (cleanupTask != null) {
            cleanupTask.stop(wait);
        }
    }

    /**
     * Removes all the entries from this cache. The cache will be empty after
     * this call returns.
     */
    public void clear() {
        lock.lock();
        try {
            cacheMap.clear();
        } finally {
            lock.unlock();
        }
    }

    private boolean isExpired(CacheEntry<V> entry) {
        final long now = System.currentTimeMillis();

        if (lifetime > 0) {
            if (now > (entry.getCreateTime() + lifetime)) {
                return true;
            }
        }
        return false;
    }

    /**
     * The cache entry object.
     *
     * The base class for value objects for caches constructed by CacheBuilder.
     * It maintains the creation time of the entry to support a lifetime
     * expiration mechanism.
     */
    private static class CacheEntry<V> {

        /* The time at which the entry was created */
        private final long createTime;
        private final V value;

        private CacheEntry(V value) {
            this.value = value;
            this.createTime = System.currentTimeMillis();
        }

        public long getCreateTime() {
            return createTime;
        }

        private V getValue() {
            return value;
        }
    }

    /**
     * Background time-based cleanup.
     *
     * Once cache instance enable the background cleanup, this task will be
     * launched periodically to look up expired value entry and removed from
     * cache.
     *
     * Notes that the background cleanup task is intensive, since it is aim to
     * look up and remove all expired value entries.
     */
    private class TimeBasedCleanupTask implements Runnable {

        private volatile boolean terminated = false;

        private final long cleanupPeriodMs;

        private final Thread cleanUpThread;

        TimeBasedCleanupTask(final long cleanupPeriodMs) {
            this.cleanupPeriodMs = cleanupPeriodMs;
            cleanUpThread = new Thread(this, "CacheCleanUpThread");
            cleanUpThread.setDaemon(true);
            cleanUpThread.start();
        }

        /**
         * Attempt to stop the background activity for the cleanup.
         * @param wait if true, the method attempts to wait for the
         * background thread to finish background activity.
         */
        void stop(boolean wait) {
            /* Set the flag to notify the run loop that it should exit */
            terminated = true;
            cleanUpThread.interrupt();

            if (wait) {
                try {
                    cleanUpThread.join();
                } catch (InterruptedException ie) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
            }
        }

        @Override
        public void run() {
            while (true) {
                if (terminated) {
                    return;
                }
                try {
                    Thread.sleep(cleanupPeriodMs);
                } catch (InterruptedException e) /* CHECKSTYLE:OFF */ {
                } /* CHECKSTYLE:ON */
                if (terminated) {
                    return;
                }
                cleanup();
            }
        }

        /**
         * Cleanup the expired entry from cache.
         */
        void cleanup() {
            if (!lock.tryLock()) {
                return;
            }
            try {
                final Iterator<Map.Entry<K, CacheEntry<V>>> iter =
                    cacheMap.entrySet().iterator();
                while (iter.hasNext()) {
                    final Map.Entry<K, CacheEntry<V>> entry =
                        iter.next();
                    if (isExpired(entry.getValue())) {
                        iter.remove();
                    }
                }
            } finally {
                lock.unlock();
            }
        }
    }
}
