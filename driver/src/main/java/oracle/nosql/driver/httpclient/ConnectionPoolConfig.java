package oracle.nosql.driver.httpclient;

public class ConnectionPoolConfig {

    private final long connectTimeout;
    private final int maxConnections;
    private final int maxPendingAcquires;
    private final long pendingAcquireTimeout;
    private final long maxIdleTime;
    private final long maxLifetime;

    private ConnectionPoolConfig(Builder builder) {
        this.connectTimeout = builder.connectTimeout;
        this.maxConnections = builder.maxConnections;
        this.maxPendingAcquires = builder.maxPendingAcquires;
        this.pendingAcquireTimeout = builder.pendingAcquireTimeout;
        this.maxIdleTime = builder.maxIdleTime;
        this.maxLifetime = builder.maxLifetime;
    }

    public long getConnectTimeout() {
        return connectTimeout;
    }

    public int getMaxConnections() {
        return maxConnections;
    }

    public int getMaxPendingAcquires() {
        return maxPendingAcquires;
    }

    public long getPendingAcquireTimeout() {
        return pendingAcquireTimeout;
    }

    public long getMaxIdleTime() {
        return maxIdleTime;
    }

    public long getMaxLifetime() {
        return maxLifetime;
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private long connectTimeout = 10000; // Default connect timeout in milliseconds
        private int maxConnections = 10; // Default max connections
        private int maxPendingAcquires = 100; // Default max pending acquires
        private long pendingAcquireTimeout = 45000; // Default pending acquire timeout in milliseconds
        private long maxIdleTime = 60000; // Default max idle time in milliseconds
        private long maxLifetime = 300000; // Default max lifetime in milliseconds

        public Builder connectTimeout(long connectTimeout) {
            this.connectTimeout = connectTimeout;
            return this;
        }

        public Builder maxConnections(int maxConnections) {
            this.maxConnections = maxConnections;
            return this;
        }

        public Builder maxPendingAcquires(int maxPendingAcquires) {
            this.maxPendingAcquires = maxPendingAcquires;
            return this;
        }

        public Builder pendingAcquireTimeout(long pendingAcquireTimeout) {
            this.pendingAcquireTimeout = pendingAcquireTimeout;
            return this;
        }

        public Builder maxIdleTime(long maxIdleTime) {
            this.maxIdleTime = maxIdleTime;
            return this;
        }

        public Builder maxLifetime(long maxLifetime) {
            this.maxLifetime = maxLifetime;
            return this;
        }

        public ConnectionPoolConfig build() {
            return new ConnectionPoolConfig(this);
        }
    }
}
