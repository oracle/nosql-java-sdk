package oracle.nosql.driver.ops;

import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.RateLimiter;
import oracle.nosql.driver.values.MapValue;

/**
 * QueryIterableResult represents an {@link Iterable} over all the query
 * results.
 *<p>
 * The shape of the values is based on the schema implied by the query. For
 * example a query such as "SELECT * FROM ..." that returns an intact row will
 * return values that conform to the schema of the table. Projections return
 * instances that conform to the schema implied by the statement. UPDATE
 * queries either return values based on a RETURNING clause or, by default,
 * the number of rows affected by the statement.
 * <p>
 * Example:
 * <pre>
 * NoSQLHandle handle = ...;
 *
 * QueryRequest qreq = new QueryRequest().setStatement("select * from foo");
 *
 * for (MapValue row : handle.queryIterable(qreq)) {
 *     // do something with row
 * }
 * </pre>
 *
 * Note: The read/write KB/Units, rate limit delay and retry stats are summed
 * up from the beginning of the iteration.
 *
 * @see NoSQLHandle#queryIterable(QueryRequest)
 */
public class QueryIterableResult
    extends Result
    implements Iterable<MapValue> {

    final QueryRequest request;
    private final NoSQLHandle handle;

    private int readKB, readUnits, writeKB, writeUnits;

    /**
     * @hidden
     * @param request the request used
     * @param handle the NoSQL handle
     */
    public QueryIterableResult(QueryRequest request, NoSQLHandle handle) {
        assert request != null : "request should not be null";
        assert handle != null : "handle should not be null";
        this.request = request;
        this.handle = handle;
    }

    /**
     * Returns an iterator over all results of a query.
     * @return the iterator
     */
    @Override
    public QueryResultIterator iterator() {
        return new QueryResultIterator(this);
    }

    /**
     * Returns the read throughput consumed by this operation, in KBytes.
     * This is the actual amount of data read by the operation. The number
     * of read units consumed is returned by {@link #getReadUnits} which may
     * be a larger number if the operation used {@link Consistency#ABSOLUTE}
     *
     * @return the read KBytes consumed
     */
    public int getReadKB() {
        return readKB;
    }

    /**
     * Returns the write throughput consumed by this operation, in KBytes.
     *
     * @return the write KBytes consumed
     */
    public int getWriteKB() {
        return writeKB;
    }

    /**
     * Returns the read throughput consumed by this operation, in read units.
     * This number may be larger than that returned by {@link #getReadKB} if
     * the operation used {@link Consistency#ABSOLUTE}
     *
     * @return the read units consumed
     */
    public int getReadUnits() {
        return readUnits;
    }

    /**
     * Returns the write throughput consumed by this operation, in write
     * units.
     *
     * @return the write units consumed
     */
    public int getWriteUnits() {
        return writeUnits;
    }


    /**
     * Implements an iterator over all results of a query.
     * Internally the driver gets a batch of rows, at a time, from the server.
     *
     * During iteration, user can adjust, at any time, the timeout, rate limiters
     * and max number of items in one batch.
     */
    public static class QueryResultIterator implements Iterator<MapValue> {
        final QueryIterableResult queryIterableResult;
        final QueryRequest internalRequest;
        Iterator<MapValue> partialResultsIterator;
        boolean closed = false;

        QueryResultIterator(QueryIterableResult queryIterableResult) {
            assert queryIterableResult != null;
            this.queryIterableResult = queryIterableResult;
            internalRequest = queryIterableResult.request.copy();
        }

        /**
         * Returns the read rate limiter instance used for batch requests.
         * Cloud service only.
         * <p>
         * This will be the value supplied via {@link #setReadRateLimiter}, or if
         * that was not called, it may be an instance of an internal rate
         * limiter that was configured internally during request processing.
         * <p>
         * This is supplied for stats and tracing/debugging only. The returned
         * limiter should be treated as read-only.
         *
         * @return the rate limiter instance used for read operations, or null
         *         if no limiter was used.
         */
        public RateLimiter getReadRateLimiter() {
            return internalRequest.getReadRateLimiter();
        }

        /**
         * Sets a read rate limiter to use for batch requests.
         * Cloud service only.
         * <p>
         * This will override any internal rate limiter that may have
         * otherwise been used during request processing, and it will be
         * used regardless of any rate limiter config.
         *
         * @param rateLimiter the rate limiter instance to use for read
         *                    operations
         */
        public void setReadRateLimiter(RateLimiter rateLimiter) {
            internalRequest.setReadRateLimiter(rateLimiter);
        }

        /**
         * Returns the write rate limiter instance used for batch requests.
         * Cloud service only.
         * <p>
         * This will be the value supplied via {@link #setWriteRateLimiter}, or
         * if that was not called, it may be an instance of an internal rate
         * limiter that was configured internally during request processing.
         * <p>
         * This is supplied for stats and tracing/debugging only. The returned
         * limiter should be treated as read-only.
         *
         * @return the rate limiter instance used for write operations, or null
         *         if no limiter was used.
         */
        public RateLimiter getWriteRateLimiter() {
            return internalRequest.getWriteRateLimiter();
        }

        /**
         * Sets a write rate limiter to use for batch requests.
         * Cloud service only.
         * <p>
         * This will override any internal rate limiter that may have
         * otherwise been used during request processing, and it will be
         * used regardless of any rate limiter config.
         *
         * @param rateLimiter the rate limiter instance to use for write
         *                    operations
         */
        public void setWriteRateLimiter(RateLimiter rateLimiter) {
            internalRequest.setWriteRateLimiter(rateLimiter);
        }

        /**
         * Sets the batch request timeout value, in milliseconds. This overrides
         * any default value set in {@link NoSQLHandleConfig}. The value must be
         * positive.
         *
         * @param timeoutMs the timeout value, in milliseconds
         *
         * @throws IllegalArgumentException if the timeout value is less than
         * or equal to 0
         */
        public void setTimeout(int timeoutMs) {
            internalRequest.setTimeout(timeoutMs);
        }

        /**
         * Returns the batch request timeout in milliseconds. A value
         * of 0 indicates that the timeout has not been set.
         *
         * @return the value
         */
        public int getTimeout() {
            return internalRequest.getTimeout();
        }

        /**
         * Returns the limit on number of items fetched by the next
         * batch operation. If not set by the application this value will be 0
         * which means no limit set.
         *
         * @return the limit, or 0 if not set
         */
        public int getLimit() {
            return internalRequest.getLimit();
        }

        /**
         * Sets the limit on number of items fetched by the next
         * batch operation. This allows an operation to return less than the
         * default amount of data.
         *
         * @param limit the limit in terms of number of items fetched at one
         *              time
         *
         * @throws IllegalArgumentException if the limit value is less than 0.
         */
        public void setLimit(int limit) {
            internalRequest.setLimit(limit);
        }

        private void compute() {
            QueryResult internalResult;
            if (partialResultsIterator == null) {
                internalResult =
                    queryIterableResult.handle.query(internalRequest);
                List<MapValue> partialResults = internalResult.getResults();
                assert partialResults != null : "partialResults should not be" +
                    " null";
                partialResultsIterator = partialResults.iterator();
                setStats(internalResult);
            }

            while (!partialResultsIterator.hasNext() &&
                !internalRequest.isDone()) {

                // get the batch of results
                internalResult =
                    queryIterableResult.handle.query(internalRequest);

                partialResultsIterator = internalResult.getResults().iterator();
                setStats(internalResult);
            }

            if (internalRequest.isDone()) {
                internalRequest.close();
                closed = true;
            }
        }

        private void setStats(QueryResult internalResult) {
            queryIterableResult.readKB += internalResult.getReadKB();
            queryIterableResult.readUnits += internalResult.getReadUnits();
            queryIterableResult.writeKB += internalResult.getWriteKB();
            queryIterableResult.writeUnits += internalResult.getWriteUnits();
            queryIterableResult.setRateLimitDelayedMs(
                queryIterableResult.getRateLimitDelayedMs() +
                    internalResult.getRateLimitDelayedMs());
            queryIterableResult.setReadKB(queryIterableResult.getReadKB() +
                internalResult.getReadKB());
            queryIterableResult.setReadUnits(
                queryIterableResult.getReadUnits() +
                    internalResult.getReadUnits());
            queryIterableResult.setWriteKB(queryIterableResult.getWriteKB()
                + internalResult.getWriteKB());

            if( internalResult.getRetryStats() != null) {
                if (queryIterableResult.getRetryStats() == null) {
                    queryIterableResult.setRetryStats(
                        new RetryStats());
                }
                queryIterableResult.getRetryStats().addDelayMs(
                    internalResult.getRetryStats().getDelayMs());
                queryIterableResult.getRetryStats().incrementRetries(
                    internalResult.getRetryStats().getRetries());
                queryIterableResult.getRetryStats().addExceptions(
                    internalResult.getRetryStats().getExceptionMap());
            }
        }

        /**
         * Returns {@code true} if the iteration has more results.
         *
         * @return {@code true} if the iteration has more results
         *
         * @throws IllegalArgumentException if any of the parameters are invalid
         * or required parameters are missing
         *
         * @throws NoSQLException if the operation cannot be performed for
         * any other reason
         */
        @Override
        public boolean hasNext() {
            if (closed) {
                return false;
            }
            compute();
            return partialResultsIterator.hasNext();
        }

        /**
         * Returns the next result for the query.
         *
         * @return the next result
         *
         * @throws NoSuchElementException if the iteration has no more results
         *
         * @throws IllegalArgumentException if any of the parameters are invalid
         * or required parameters are missing
         *
         * @throws NoSQLException if the operation cannot be performed for
         * any other reason
         */
        @Override
        public MapValue next() {
            if (closed) {
                throw new NoSuchElementException("Iterator already closed.");
            }
            compute();
            return partialResultsIterator.next();
        }

        /**
         * Terminates the query execution and releases any memory consumed by
         * the query at the driver. An application should use this method if it
         * wishes to terminate query execution before retrieving all of the
         * query results.
         */
        public void close() {
            closed = true;
            internalRequest.close();
        }
    }
}