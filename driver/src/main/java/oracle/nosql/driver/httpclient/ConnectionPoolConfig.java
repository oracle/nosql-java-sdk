package oracle.nosql.driver.httpclient;

/**
 * Configuration for building connection pool.
 */
public class ConnectionPoolConfig {
    private final int maxConnections;
    private final int maxPendingAcquires;
    private final long pendingAcquireTimeout;
    private final long maxIdleTime;
    private final long maxLifetime;

    private ConnectionPoolConfig(Builder builder) {
        this.maxConnections = builder.maxConnections;
        this.maxPendingAcquires = builder.maxPendingAcquires;
        this.pendingAcquireTimeout = builder.pendingAcquireTimeout;
        this.maxIdleTime = builder.maxIdleTime;
        this.maxLifetime = builder.maxLifetime;
    }

    /**
     * Get the maximum number of connections that can be created by the
     * connection pool.
     *
     * @return maximum number of connections created by the connection pool.
     */
    public int getMaxConnections() {
        return maxConnections;
    }

    /**
     * Get the maximum number of requests for connection acquire to keep in a
     * pending queue.
     *
     * @return maximum number of pending request for connection acquisition
     */
    public int getMaxPendingAcquires() {
        return maxPendingAcquires;
    }

    /**
     * Get the timeout in milliseconds to wait for connection acquisition.
     *
     * @return timeout in milliseconds to wait for connection acquisition.
     */
    public long getPendingAcquireTimeout() {
        return pendingAcquireTimeout;
    }

    /**
     * Get the maximum idle time in milliseconds for the idle connection to be
     * closed.
     *
     * @return maximum idle time in milliseconds for the idle connection to be
     * closed.
     */
    public long getMaxIdleTime() {
        return maxIdleTime;
    }

    /**
     * Ge the maximum time in milliseconds for the connection to close.
     *
     * @return maximum time in milliseconds for the connection to close.
     */
    public long getMaxLifetime() {
        return maxLifetime;
    }

    /**
     * Builder
     * @return Builder instance
     */
    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {
        /*TODO move defaults to constants with System property similar to
         * reactor netty
         */
        private int maxConnections = 100;
        private int maxPendingAcquires = -1; // Default max pending acquires
        private long pendingAcquireTimeout = 45000; // Default pending acquire timeout in milliseconds
        private long maxIdleTime = 60000; // Default max idle time in milliseconds
        private long maxLifetime = 300000; // Default max lifetime in milliseconds

        /**
         * Set the maximum number of channels to create before start pending.
         * Default to TODO
         *
         * @param maxConnections the maximum number of connections in the pool
         * @return this
         * @throws IllegalArgumentException if maxConnections is not positive
         */
        public Builder maxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            if (maxConnections <= 0) {
                throw new IllegalArgumentException("maxConnections must be " +
                        "positive, provided value is " + maxConnections);
            }
            return this;
        }

        /**
         * Set the maximum number of registered requests for connection acquire
         * to keep in a pending queue.
         * Default to -1 which means the pending queue will not have upper limit.
         *
         * @param maxPendingAcquires the maximum number of registered requests
         *                           for acquire to keep in a pending queue.
         * @return this
         * @throws IllegalArgumentException If pendingAcquireMaxCount is zero
         * or less than -1
         */
        public Builder maxPendingAcquires(int maxPendingAcquires) {
            this.maxPendingAcquires = maxPendingAcquires;
            if (maxPendingAcquires != -1 && maxPendingAcquires <= 0) {
                throw new IllegalArgumentException("maxPendingAcquires must " +
                    "be positive or -1, provided value is " + maxPendingAcquires);
            }
            return this;
        }

        /**
         * Set the maximum time after which a pending connection acquire must
         * be complete or the {@link java.util.concurrent.TimeoutException}
         * will be thrown (resolution: ms).
         * Default to 45000
         *
         * @param pendingAcquireTimeout maximum time in milliseconds to wait
         *                              for the acquisition of the channel
         * @return this
         * @throws IllegalArgumentException If pendingAcquireTimeout is not
         * positive
         */
        public Builder pendingAcquireTimeout(long pendingAcquireTimeout) {
            this.pendingAcquireTimeout = pendingAcquireTimeout;
            if (pendingAcquireTimeout <= 0) {
                throw new IllegalArgumentException("pendingAcquireTimeout " +
                    "must be positive, provided value is " + pendingAcquireTimeout);
            }
            return this;
        }

        /**
         * Set the time in milliseconds after which channel will be closed
         * when idle.
         * Default to 60000 TODO
         * @param maxIdleTime Time after which the channel will be
         *                    closed when idle. The check is performed only
         *                    when the Channel is selected for use.
         * @return this
         * @throws IllegalArgumentException If maxIdleTime is not positive
         */
        public Builder maxIdleTime(long maxIdleTime) {
            this.maxIdleTime = maxIdleTime;
            if (maxIdleTime <= 0) {
                throw new IllegalArgumentException("maxIdleTime must be " +
                    "positive, provided value is " + maxIdleTime);
            }
            return this;
        }

        /**
         * Set the time in milliseconds after which the channel will be closed.
         * Default to 300000 TODO
         *
         * @param maxLifetime Time after which the channel will be closed.
         *                    The check is performed only when channel is
         *                    selected for use.
         * @return this
         * @throws IllegalArgumentException If maxLifetime is not positive
         */
        public Builder maxLifetime(long maxLifetime) {
            this.maxLifetime = maxLifetime;
            if (maxLifetime <= 0) {
                throw new IllegalArgumentException("maxLifetime must be " +
                    "positive, provided value is " + maxLifetime);
            }
            return this;
        }

        /**
         * Build the {@link ConnectionPoolConfig} instance
         *
         * @return new {@link ConnectionPoolConfig}
         */
        public ConnectionPoolConfig build() {
            return new ConnectionPoolConfig(this);
        }
    }
}
