/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.Set;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
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
 * try (QueryRequest qreq = new QueryRequest()
 *          .setStatement("select * from foo") ) {
 *
 *     for (MapValue row : handle.queryIterable(qreq)) {
 *         // do something with row
 *     }
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
    implements Iterable<MapValue>, AutoCloseable {

    private final QueryRequest request;
    private Set<QueryResultIterator> unclosedIters;

    private final NoSQLHandle handle;
    private boolean firstIteratorCall = true;

    private int readKB, readUnits, writeKB, writeUnits;

    /**
     * Internal use only
     * @param request the request used
     * @param handle the NoSQL handle
     * @hidden
     */
    public QueryIterableResult(QueryRequest request, NoSQLHandle handle) {
        assert request != null : "request should not be null";
        assert handle != null : "handle should not be null";
        if (request.getContKey() != null) {
            throw new IllegalArgumentException("A new QueryRequest is " +
                "required for a QueryIterableResult.");
        }
        this.request = request;
        this.handle = handle;
    }

    /**
     * Returns an iterator over all results of a query. Each call is treated
     * as a separate query. The first server call is done at the time of the
     * first hasNext()/next() call.
     * Note: Objects returned by this method can only be used safely by one
     * thread at a time unless synchronized externally.
     *
     * @return the iterator
     */
    @Override
    public Iterator<MapValue> iterator() {
        QueryResultIterator resultIterator;
        if (unclosedIters == null) {
            unclosedIters = new HashSet<>();
        }
        if (firstIteratorCall) {
            resultIterator = new QueryResultIterator(request);
            firstIteratorCall = false;
        } else {
            QueryRequest requestCopy = request.copy();
            resultIterator = new QueryResultIterator(requestCopy);
        }
        unclosedIters.add(resultIterator);
        return resultIterator;
    }

    /*
     * Used internally to remove tracking of unclosed iterators
     * resources.
     */
    private void removeTracking(QueryResultIterator iter) {
        if (unclosedIters != null) {
            unclosedIters.remove(iter);
        }
    }

    /**
     * Returns the read throughput consumed by all iterators of this operation,
     * in  KBytes. This is the actual amount of data read by this operation.
     * The number of read units consumed is returned by {@link #getReadUnits}
     * which may be a larger number if the operation used
     * {@link Consistency#ABSOLUTE}
     *
     * @return the read KBytes consumed
     */
    public int getReadKB() {
        return readKB;
    }

    /**
     * Returns the write throughput consumed by all iterators of this operation,
     * in KBytes.
     *
     * @return the write KBytes consumed
     */
    public int getWriteKB() {
        return writeKB;
    }

    /**
     * Returns the read throughput consumed by all iterators of this operation,
     * in read units.
     * This number may be larger than that returned by {@link #getReadKB} if
     * the operation used {@link Consistency#ABSOLUTE}
     *
     * @return the read units consumed
     */
    public int getReadUnits() {
        return readUnits;
    }

    /**
     * Returns the write throughput consumed by all iterators of this operation,
     * in write units.
     *
     * @return the write units consumed
     */
    public int getWriteUnits() {
        return writeUnits;
    }

    @Override
    public void close() {
        if (unclosedIters != null) {
            unclosedIters.forEach(iter -> iter.close());
        }
    }

    /**
     * Implements an iterator over all results of a query.
     * Internally the driver gets a batch of rows, at a time, from the server.
     */
    private class QueryResultIterator implements Iterator<MapValue> {
        final QueryRequest internalRequest;
        Iterator<MapValue> partialResultsIterator;
        boolean closed = false;

        QueryResultIterator(QueryRequest queryRequest) {
            this.internalRequest = queryRequest;
        }

        private void compute() {
            QueryResult internalResult;
            if (partialResultsIterator == null) {
                internalResult =
                    handle.query(internalRequest);
                List<MapValue> partialResults = internalResult.getResults();
                partialResultsIterator = partialResults.iterator();
                setStats(internalResult);
            }

            while (!partialResultsIterator.hasNext() &&
                !internalRequest.isDone()) {

                // get the batch of results
                internalResult = handle.query(internalRequest);

                partialResultsIterator = internalResult.getResults().iterator();
                setStats(internalResult);
            }

            if (internalRequest.isDone() && !partialResultsIterator.hasNext()) {
                close();
            }
        }

        private void setStats(QueryResult internalResult) {
            readKB += internalResult.getReadKB();
            readUnits += internalResult.getReadUnits();
            writeKB += internalResult.getWriteKB();
            writeUnits += internalResult.getWriteUnits();
            setRateLimitDelayedMs(
                getRateLimitDelayedMs() +
                    internalResult.getRateLimitDelayedMs());
            setReadKB(getReadKB() +
                internalResult.getReadKB());
            setReadUnits(
                getReadUnits() +
                    internalResult.getReadUnits());
            setWriteKB(getWriteKB()
                + internalResult.getWriteKB());

            if(internalResult.getRetryStats() != null) {
                if (getRetryStats() == null) {
                    setRetryStats(
                        new RetryStats());
                }
                getRetryStats().addStats(
                    internalResult.getRetryStats());
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
            removeTracking(this);
        }
    }
}
