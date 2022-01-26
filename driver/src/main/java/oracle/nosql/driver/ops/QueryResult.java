/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.util.Iterator;
import java.util.List;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.RateLimiter;
import oracle.nosql.driver.http.Client;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.values.MapValue;

/**
 * QueryResult comprises a list of {@link MapValue} instances representing the
 * query results.
 *<p>
 * The shape of the values is based on the schema implied by the query. For
 * example a query such as "SELECT * FROM ..." that returns an intact row will
 * return values that conform to the schema of the table. Projections return
 * instances that conform to the schema implied by the statement. UPDATE
 * queries either return values based on a RETURNING clause or, by default,
 * the number of rows affected by the statement.
 * <p>
 * A single QueryResult does not imply that all results for the query have been
 * returned. If the value returned by {@link #getContinuationKey} is not null
 * there are additional results available. This can happen even if there are
 * no values in the returned QueryResult. The best way to use
 * {@link QueryRequest} and QueryResult is to perform operations in a loop,
 * for example:
 * <pre>
 * NoSQLHandle handle = ...;
 *
 * QueryRequest qreq = new QueryRequest().setStatement("select * from foo");
 *
 * do {
 *   QueryResult qres = handle.query(qreq);
 *   List&lt;MapValue&gt; results = qres.getResults();
 *   // do something with the results
 * } while (!qreq.isDone())
 * </pre>
 *
 * @see NoSQLHandle#query
 */
public class QueryResult extends Result {

    private final QueryRequest request;

    private List<MapValue> results;

    private byte[] continuationKey;

    /*
     * The following 6 fields are used only for "internal" QueryResults, i.e.,
     * those received and processed by the ReceiveIter.
     */

    private boolean reachedLimit;

    private boolean isComputed;

    /*
     * The following 4 fields are used during phase 1 of a sorting ALL_PARTITIONS
     * query (see javadoc of PartitionUnionIter in kvstore for description of
     * the algorithm used to execute such queries). In this case, the results may
     * store results from multiple partitions. If so, the results are grouped by
     * partition and the pids, numResultsPerPid, and continuationKeys fields
     * store the partition id, the number of results, and the continuation key
     * per partition. Finally, the isInPhase1 specifies whether phase 1 is done.
     */
    private boolean isInPhase1;

    private int[] pids;

    private int[] numResultsPerPid;

    private byte[][] continuationKeys;
    private Client client;

    /**
     * @hidden
     * @param req the request used
     */
    public QueryResult(QueryRequest req) {
        this(req, true);
    }

    /**
     * @hidden
     * @param req the request used
     * @param computed whether query has been computed or not
     */
    public QueryResult(QueryRequest req, boolean computed) {
        super();
        request = req;
        isComputed = computed;
    }

    /**
     * @hidden
     * @param v set value of computed
     * @return this
     */
    public QueryResult setComputed(boolean v) {
        isComputed = v;
        return this;
    }

    /**
     * @hidden
     * @return the original query request
     */
    public QueryRequest getRequest() {
        return request;
    }

    /**
     * @hidden
     * @return whether the limit has been reached
     */
    public boolean reachedLimit() {
        return reachedLimit;
    }

    /**
     * @hidden
     * @param v whether the limit has been reached
     * @return this
     */
    public QueryResult setReachedLimit(boolean v) {
        reachedLimit = v;
        return this;
    }

    /**
     * @hidden
     * @param pids partition ids
     * @return this
     */
    public QueryResult setPids(int[] pids) {
        this.pids = pids;
        return this;
    }

    /**
     * @hidden
     * @return the number of partition ids
     */
    public int getNumPids() {
        return (pids == null ? 0 : pids.length);
    }

    /**
     * @hidden
     * @param i the index to use
     * @return the partition id at index i
     */
    public int getPid(int i) {
        return pids[i];
    }

    /**
     * @hidden
     * @param v the array of number of results per partition
     * @return this
     */
    public QueryResult setNumResultsPerPid(int[] v) {
        numResultsPerPid = v;
        return this;
    }

    /**
     * @hidden
     * @param i the index to use
     * @return the number of results for the specified partition
     */
    public int getNumPartitionResults(int i) {
        return numResultsPerPid[i];
    }

    /**
     * @hidden
     * @param i the index to use
     * @return the continuation key for the specified partition
     */
    public byte[] getPartitionContKey(int i) {
        return continuationKeys[i];
    }

    /**
     * @hidden
     * @param keys array of continuation keys
     * @return this
     */
    public QueryResult setPartitionContKeys(byte[][] keys) {
        continuationKeys = keys;
        return this;
    }

    /**
     * @hidden
     * @param v value to set
     * @return this;
     */
    public QueryResult setIsInPhase1(boolean v) {
        isInPhase1 = v;
        return this;
    }

    /**
     * @hidden
     * @return true if the query is in phase 1
     */
    public boolean isInPhase1() {
        return isInPhase1;
    }

    /**
     * @hidden
     */
    void compute() {

        if (isComputed) {
            return;
        }

        QueryDriver driver = request.getDriver();
        driver.compute(this);
        isComputed = true;

        /*
         * If the original request specified rate limiting, apply the
         * used read/write units to the limiter(s) here
         */
        if (request != null) {
            RateLimiter readLimiter = request.getReadRateLimiter();
            if (readLimiter != null) {
                readLimiter.consumeUnitsUnconditionally(
                    super.getReadUnitsInternal());
            }
            RateLimiter writeLimiter = request.getWriteRateLimiter();
            if (writeLimiter != null) {
                writeLimiter.consumeUnitsUnconditionally(
                    super.getWriteUnitsInternal());
            }
        }
    }

    /**
     * @hidden
     * @return query results
     */
    public List<MapValue> getResultsInternal() {
        return results;
    }

    /**
     * Returns a list of results for the query. It is possible to have an empty
     * list and a non-null continuation key.
     *
     * @return the query results
     */
    public List<MapValue> getResults() {
        compute();
        return results;
    }

    /**
     * @hidden
     * @param results the results
     * @return this
     */
    public QueryResult setResults(List<MapValue> results) {
        this.results = results;
        return this;
    }

    /**
     * Returns the continuation key that can be used to obtain more results
     * if non-null.
     *
     * @return the continuation key, or null if there are no further values to
     * return.
     */
    public byte[] getContinuationKey() {
        compute();
        return continuationKey;
    }

    /**
     * @hidden
     * @param continuationKey the continuation key
     * @return this
     */
    public QueryResult setContinuationKey(byte[] continuationKey) {
        this.continuationKey = continuationKey;
        return this;
    }

    /* from Result */

    /**
     * Returns the read throughput consumed by this operation, in KBytes.
     * This is the actual amount of data read by the operation. The number
     * of read units consumed is returned by {@link #getReadUnits} which may
     * be a larger number if the operation used {@link Consistency#ABSOLUTE}
     *
     * @return the read KBytes consumed
     */
    public int getReadKB() {
        compute();
        return super.getReadKBInternal();
    }

    /**
     * Returns the write throughput consumed by this operation, in KBytes.
     *
     * @return the write KBytes consumed
     */
    public int getWriteKB() {
        compute();
        return super.getWriteKBInternal();
    }

    /**
     * Returns the read throughput consumed by this operation, in read units.
     * This number may be larger than that returned by {@link #getReadKB} if
     * the operation used {@link Consistency#ABSOLUTE}
     *
     * @return the read units consumed
     */
    public int getReadUnits() {
        compute();
        return super.getReadUnitsInternal();
    }

    /**
     * Returns the write throughput consumed by this operation, in write
     * units.
     *
     * @return the write units consumed
     */
    public int getWriteUnits() {
        compute();
        return super.getWriteUnitsInternal();
    }

    @Override
    public String toString() {

        compute();

        if (results == null) {
            return null;
        }

        StringBuffer sb = new StringBuffer();
        sb.append("Number of query results: ").append(results.size());
        for (MapValue res : results) {
            sb.append("\n").append(res);
        }
        sb.append("\n");
        return sb.toString();
    }

    public QueryResult copy(QueryRequest newRequest) {
        QueryResult copy = new QueryResult(newRequest, isComputed);
        //copy.results = new ArrayList<>(results);
        copy.continuationKey = continuationKey;
        copy.reachedLimit = reachedLimit;
        copy.isInPhase1 = isInPhase1;
        copy.pids = pids;
        copy.numResultsPerPid = numResultsPerPid;
        copy.continuationKeys = continuationKeys;
        copy.client = client;
        return copy;
    }

    public ResultsIterable getAllResults() {
        return new ResultsIterable(this);
    }

    public void setClient(Client client) {
        this.client = client;
    }

    public Client getClient() {
        return client;
    }

    public static class ResultsIterable implements Iterable<MapValue> {
        final QueryResult internalResult;
        private int readKB, readUnits, writeKB, writeUnits;

        ResultsIterable(QueryResult queryResult) {
            QueryRequest internalRequest = queryResult.request.copy();
            if (internalRequest.isSimpleQuery()) {
                // at this time the simple query result has already the first
                // batch of results already computed
                this.internalResult = (QueryResult)
                    queryResult.getClient().execute(internalRequest);
            } else {
                this.internalResult = new QueryResult(internalRequest, false);
            }
        }

        @Override
        public Iterator<MapValue> iterator() {
            return new ResultsIterator(this);
        }

        public RateLimiter getReadRateLimiter() {
            return internalResult.request.getReadRateLimiter();
        }

        public void setReadLimiter(RateLimiter rateLimiter) {
            internalResult.request.setReadRateLimiter(rateLimiter);
        }

        public RateLimiter getWriteRateLimiter() {
            return internalResult.request.getWriteRateLimiter();
        }

        public void setWriteRateLimiter(RateLimiter rateLimiter) {
            internalResult.request.setWriteRateLimiter(rateLimiter);
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
            return internalResult.getReadKB();
        }

        /**
         * Returns the write throughput consumed by this operation, in KBytes.
         *
         * @return the write KBytes consumed
         */
        public int getWriteKB() {
            return internalResult.getWriteKB();
        }

        /**
         * Returns the read throughput consumed by this operation, in read units.
         * This number may be larger than that returned by {@link #getReadKB} if
         * the operation used {@link Consistency#ABSOLUTE}
         *
         * @return the read units consumed
         */
        public int getReadUnits() {
            return internalResult.getReadUnits();
        }

        /**
         * Returns the write throughput consumed by this operation, in write
         * units.
         *
         * @return the write units consumed
         */
        public int getWriteUnits() {
            return internalResult.getWriteUnits();
        }

        // get/setTimeout ???
    }

    private static class ResultsIterator implements Iterator<MapValue> {
        final ResultsIterable resultsIterable;
        final QueryRequest internalRequest;
        QueryResult internalResult;
        Iterator<MapValue> partialResultsIterator;

        ResultsIterator(ResultsIterable resultsIterable) {
            this.resultsIterable = resultsIterable;
            internalRequest = resultsIterable.internalResult.request;
            internalResult = resultsIterable.internalResult;
        }

        private void compute() {
            if (partialResultsIterator == null) {
                List<MapValue> partialResults = internalResult.getResults();
                if (partialResults != null) {
                    partialResultsIterator = partialResults.iterator();
                } else {
                    return;
                }
            }

            while (!partialResultsIterator.hasNext() &&
                !internalRequest.isDone()) {
                if (internalRequest.isSimpleQuery()) {
                    Client client = internalResult.getClient();
                    internalResult =
                        (QueryResult)(client.execute(internalRequest));
                } else {
                    internalResult.isComputed = false;
                    internalResult.compute();
                }
                partialResultsIterator = internalResult.getResults().iterator();
                resultsIterable.readKB += internalResult.getReadKB();
                resultsIterable.readUnits += internalResult.getReadUnits();
                resultsIterable.writeKB += internalResult.getWriteKB();
                resultsIterable.writeUnits += internalResult.getWriteUnits();
            }

            if (internalRequest.isDone()) {
                internalRequest.close();
            }
        }

        @Override
        public boolean hasNext() {
            compute();
            return partialResultsIterator.hasNext();
        }

        @Override
        public MapValue next() {
            compute();
            return partialResultsIterator.next();
        }
    }


}
