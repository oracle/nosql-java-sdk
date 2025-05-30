/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.util.List;
import java.util.Map;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.query.VirtualScan;
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
 * try (QueryRequest qreq = new QueryRequest()
 *   .setStatement("select * from * foo")) {
 *
 *   do {
 *     QueryResult qres = handle.query(qreq);
 *     List&lt;MapValue&gt; results = qres.getResults();
 *     // do something with the results
 *   } while (!qreq.isDone());
 * }
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

    private VirtualScan[] virtualScans;

    private Map<String, String> queryTraces;

    /**
     * internal use only
     * @param req the request used
     * @hidden
     */
    public QueryResult(QueryRequest req) {
        this(req, true);
    }

    /**
     * internal use only
     * @param req the request used
     * @param computed whether query has been computed or not
     * @hidden
     */
    public QueryResult(QueryRequest req, boolean computed) {
        super();
        request = req;
        isComputed = computed;
    }

    /**
     * internal use only
     * @param v set value of computed
     * @return this
     * @hidden
     */
    public QueryResult setComputed(boolean v) {
        isComputed = v;
        return this;
    }

    /**
     * internal use only
     * @return the original query request
     * @hidden
     */
    public QueryRequest getRequest() {
        return request;
    }

    /**
     * internal use only
     * @return whether the limit has been reached
     * @hidden
     */
    public boolean reachedLimit() {
        return reachedLimit;
    }

    /**
     * internal use only
     * @param v whether the limit has been reached
     * @return this
     * @hidden
     */
    public QueryResult setReachedLimit(boolean v) {
        reachedLimit = v;
        return this;
    }

    /**
     * internal use only
     * @param pids partition ids
     * @return this
     * @hidden
     */
    public QueryResult setPids(int[] pids) {
        this.pids = pids;
        return this;
    }

    /**
     * internal use only
     * @return the number of partition ids
     * @hidden
     */
    public int getNumPids() {
        return (pids == null ? 0 : pids.length);
    }

    /**
     * internal use only
     * @param i the index to use
     * @return the partition id at index i
     * @hidden
     */
    public int getPid(int i) {
        return pids[i];
    }

    /**
     * internal use only
     * @param v the array of number of results per partition
     * @return this
     * @hidden
     */
    public QueryResult setNumResultsPerPid(int[] v) {
        numResultsPerPid = v;
        return this;
    }

    /**
     * internal use only
     * @param i the index to use
     * @return the number of results for the specified partition
     * @hidden
     */
    public int getNumPartitionResults(int i) {
        return numResultsPerPid[i];
    }

    /**
     * internal use only
     * @param i the index to use
     * @return the continuation key for the specified partition
     * @hidden
     */
    public byte[] getPartitionContKey(int i) {
        return continuationKeys[i];
    }

    /**
     * internal use only
     * @param keys array of continuation keys
     * @return this
     * @hidden
     */
    public QueryResult setPartitionContKeys(byte[][] keys) {
        continuationKeys = keys;
        return this;
    }

    /**
     * internal use only
     * @param v value to set
     * @return this;
     * @hidden
     */
    public QueryResult setIsInPhase1(boolean v) {
        isInPhase1 = v;
        return this;
    }

    /**
     * internal use only
     * @return true if the query is in phase 1
     * @hidden
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
    }

    /**
     * internal use only
     * @return query results
     * @hidden
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
     * internal use only
     * @param results the results
     * @return this
     * @hidden
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
     * internal use only
     * @param continuationKey the continuation key
     * @return this
     * @hidden
     */
    public QueryResult setContinuationKey(byte[] continuationKey) {
        this.continuationKey = continuationKey;
        return this;
    }

    /**
     * internal use only
     * @return the array of virtual scans
     * @hidden
     */
    public VirtualScan[] getVirtualScans() {
        return virtualScans;
    }

    /**
     * internal use only
     * @param vs the array of virtual scans
     * @hidden
     */
    public void setVirtualScans(VirtualScan[] vs) {
        virtualScans = vs;
    }

    /**
     * internal use only
     * @param traces the traces to set
     * @hidden
     */
    public void setQueryTraces(Map<String, String> traces) {
        queryTraces = traces;
    }

    /**
     * internal use only
     * @return the traces set
     * @hidden
     */
    public Map<String, String> getQueryTraces() {
        return queryTraces;
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
}
