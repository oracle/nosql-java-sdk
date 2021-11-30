/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.ByteArrayOutputStream;
import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.TreeSet;

import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.RetryableException;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.values.BinaryValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.util.SizeOf;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;

/**
 * ReceiveIter requests and receives results from the proxy. For sorting
 * queries, it performs a merge sort of the received results. It also
 * performs duplicate elimination for queries that require it (note:
 * a query can do both sorting and dup elimination).
 */
public class ReceiveIter extends PlanIter {

    public enum DistributionKind {
        /*
         * The query predicates specify a complete shard key, and as a result,
         * the query goes to a single partition and uses the primary index for
         * its execution.
         */
        SINGLE_PARTITION,
        /*
         * The query uses the primary index for its execution, but does not
         * specify a complete shard key. As a result, it must be sent to all
         * partitions.
         */
        ALL_PARTITIONS,
        /*
         * The query uses a secondary index for its execution. As a result,
         * it must be sent to all shards.
         */
        ALL_SHARDS
    }

    private static class ReceiveIterState extends PlanIterState {

        /*
         * It stores the set of shard ids. Needed for sorting all-shard
         * queries only.
         */
        TopologyInfo theTopoInfo;

        /*
         * The remote scanner used for non-sorting queries.
         */
        RemoteScanner theScanner;

        /*
         * The remote scanners used for sorting queries. For all-shard queries
         * there is one RemoreScanner per shard. For all-partition queries
         * a RemoteScanner is created for each partition that has at least one
         * result. See the javadoc of PartitionUnionIter in kvstore for a
         * description of how all-partition queries are executed in the 3-tier
         * architecture.
         */
        TreeSet<RemoteScanner> theSortedScanners;

        /*
         * Used for sorting all-partition queries. It specifies whether the
         * query execution is in sort pahse 1 (see javadoc of PartitionUnionIter
         * in kvstore).
         */
        boolean theIsInSortPhase1 = true;

        /*
         * The continuation key to be used for the next batch request during
         * sort-phase-1 of a sorting, all-partition query.
         */
        byte[] theContinuationKey = null;

        /*
         * Hash set used for duplicate elimination. It stores the primary
         * keys (in binary format) of all the results seen so far.
         */
        HashSet<BinaryValue> thePrimKeysSet;

        /*
         * The memory consumed by this ReceiveIter. Memory consumption is
         * counted for sorting all-partiton queries and/or queries that do
         * duplicate elimination. We count the memory taken by results cached
         * in theSortedScanners and/or primary keys stored in thePrimKeysSet.
         */
        long theMemoryConsumption;

        /*
         * The memory consumed for duplicate elimination
         */
        long theDupElimMemory;

        /*
         * theTotalResultsSize and theTotalNumResults store the total size
         * and number of results fetched by this ReceiveIter so far. They
         * are used to compute the average result size, which is then used
         * to compute the max number of results to fetch from a partition
         * during a sort-phase-2 request for a sorting, all-partition query.
         */
        long theTotalResultsSize;

        long theTotalNumResults;

        ReceiveIterState(RuntimeControlBlock rcb, ReceiveIter iter) {

            theTopoInfo = rcb.getTopologyInfo();

            if (iter.doesDupElim()) {
                thePrimKeysSet = new HashSet<BinaryValue>(1000);
            }

            if (iter.doesSort() &&
                iter.theDistributionKind == DistributionKind.ALL_PARTITIONS) {
                theSortedScanners = new TreeSet<RemoteScanner>();
            } else if (iter.doesSort() &&
                       iter.theDistributionKind == DistributionKind.ALL_SHARDS) {
                int numShards = theTopoInfo.numShards();
                theSortedScanners = new TreeSet<RemoteScanner>();
                for (int i = 0; i < numShards; ++i) {
                    theSortedScanners.add(
                        iter.new RemoteScanner(rcb, this, true,
                                               theTopoInfo.getShardId(i)));
                }
            } else {
                theScanner = iter.new RemoteScanner(rcb, this, false, -1);
            }
        }

        @Override
        public void done() {
            super.done();
            clear();
        }

        @Override
        public void close() {
            super.close();
            thePrimKeysSet = null;
            theSortedScanners = null;
        }

        void clear() {
            if (thePrimKeysSet != null) {
                thePrimKeysSet.clear();
            }

            if (theSortedScanners != null) {
                theSortedScanners.clear();
            }
        }
    }

    /*
     * The distribution kind of the query.
     */
    private final DistributionKind theDistributionKind;

    /*
     * Used for sorting queries. It specifies the names of the top-level
     * fields that contain the values on which to sort the received results.
     */
    private final String[] theSortFields;

    private final SortSpec[] theSortSpecs;

    /*
     * Used for duplicate elimination. It specifies the names of the top-level
     * fields that contain the primary-key values within the received results .
     */
    private final String[] thePrimKeyFields;

    public ReceiveIter(
        ByteInputStream in,
        short serialVersion) throws IOException {

        super(in, serialVersion);

        short ordinal = in.readShort();
        theDistributionKind = DistributionKind.values()[ordinal];

        theSortFields = SerializationUtil.readStringArray(in);
        theSortSpecs = readSortSpecs(in);
        thePrimKeyFields = SerializationUtil.readStringArray(in);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.RECV;
    }

    boolean doesSort() {
        return theSortFields != null;
    }

    boolean doesDupElim() {
        return thePrimKeyFields != null;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        ReceiveIterState state = new ReceiveIterState(rcb, this);
        rcb.setState(theStatePos, state);
        rcb.incMemoryConsumption(state.theMemoryConsumption);

        QueryRequest qreq = rcb.getRequest();
        assert(qreq.isPrepared());
        assert(qreq.hasDriver());
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        throw new IllegalStateException("Should never be called");
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        ReceiveIterState state = (ReceiveIterState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        ReceiveIterState state = (ReceiveIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            if (rcb.getTraceLevel() >= 1) {
                rcb.trace("ReceiveIter.next() : done");
            }
            return false;
        }

        if (!doesSort()) {
            return simpleNext(rcb, state);
        }

        return sortingNext(rcb, state);
    }

    private boolean simpleNext(
        RuntimeControlBlock rcb,
        ReceiveIterState state) {

        do {
            MapValue res = state.theScanner.next();

            if (res != null) {
                if (rcb.getTraceLevel() >= 1) {
                    rcb.trace("ReceiveIter.simpleNext() : got result :\n" + res);
                }

                if (checkDuplicate(rcb, state, res)) {
                    continue;
                }

                rcb.setRegVal(theResultReg, res);
                return true;
            }

            break;

        } while (true);

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("ReceiveIter.simleNext() : no result. Reached limit = " +
                      rcb.reachedLimit());
        }

        if (!rcb.reachedLimit()) {
            state.done();
        }

        return false;
    }

    private boolean sortingNext(
        RuntimeControlBlock rcb,
        ReceiveIterState state) {

        if (theDistributionKind == DistributionKind.ALL_PARTITIONS &&
            state.theIsInSortPhase1) {

            initPartitionSort(rcb, state);
            return false;
        }

        while (true) {
            RemoteScanner scanner = state.theSortedScanners.pollFirst();

            if (scanner == null) {
                state.done();
                return false;
            }

            MapValue res = scanner.nextLocal();

            if (res != null) {

                if (rcb.getTraceLevel() >= 1) {
                    rcb.trace("ReceiveIter.sortingNext() : got result :\n" + res);
                }

                res.convertEmptyToNull();
                rcb.setRegVal(theResultReg, res);

                if (!scanner.isDone()) {
                    state.theSortedScanners.add(scanner);
                } else {
                    if (rcb.getTraceLevel() >= 1) {
                        rcb.trace("ReceiveIter.sortingNext() : done with " +
                                  "partition/shard " + scanner.theShardOrPartId);
                    }
                }

                if (checkDuplicate(rcb, state, res)) {
                    continue;
                }

                return true;
            }

            /*
             * Scanner had no cached results. If it may have remote results,
             * send a request to fetch more results. Otherwise, throw it away
             * (by leaving it outside theSortedScanners) and continue with
             * another scanner.
             */
            if (!scanner.isDone()) {
                try {
                    scanner.fetch();
                } catch (RetryableException e) {
                    state.theSortedScanners.add(scanner);
                    throw e;
                }
            } else {
                continue;
            }

            /*
             * We executed a remote fetch. If we got any result or the scanner
             * may have more remote results, put the scanner back into
             * theSortedScanner. Otherwise, throw it away.
             */
            if (!scanner.isDone()) {
                state.theSortedScanners.add(scanner);
            } else {
                if (rcb.getTraceLevel() >= 1) {
                    rcb.trace("ReceiveIter.sortingNext() : done with " +
                              "partition/shard " + scanner.theShardOrPartId);
                }
            }

            handleTopologyChange(rcb, state);

            /*
             * For simplicity, we don't want to allow the possibility of
             * another remote fetch during the same batch, so whether or not
             * the batch limit was reached during the above fetch, we set
             * limit flag to true and return false, thus terminating the
             * current batch.
             */
            rcb.setReachedLimit(true);
            return false;
        }
    }

    /**
     * Execute a copy of a request.
     * After execution, copy specific fields from the request copy back
     * to the original request. This is needed because client.execute() may
     * set or update specific fields in the request, such as startTime,
     * retry stats, rate limiters, etc.
     */
    private QueryResult execute(RuntimeControlBlock rcb,
                                QueryRequest origRequest,
                                QueryRequest reqCopy) {
        NoSQLException e = null;
        QueryResult result = null;
        try {
            result = (QueryResult)rcb.getClient().execute(reqCopy);
        } catch (NoSQLException qe) {
            e = qe;
        }
        /*
         * Copy values back to original request, even when the execute()
         * throws an error. This is because even in the error case the
         * execute() call may have set/updated these values.
         */
        origRequest.setRetryStats(reqCopy.getRetryStats());
        origRequest.setRateLimitDelayedMs(reqCopy.getRateLimitDelayedMs());
        origRequest.setReadRateLimiter(reqCopy.getReadRateLimiter());
        origRequest.setWriteRateLimiter(reqCopy.getWriteRateLimiter());
        origRequest.setStartTimeMs(reqCopy.getStartTimeMs());
        /* if the execute() call threw an error, throw it here */
        if (e != null) {
            throw e;
        }
        return result;
    }

    /*
     * Make sure we receive (and cache) at least one result per partition
     * (except from partitions that do not contain any results at all).
     */
    private void initPartitionSort(
        RuntimeControlBlock rcb,
        ReceiveIterState state) {

        assert(state.theIsInSortPhase1);

        /*
         * Create and execute a request to get at least one result from
         * the partition whose id is specified in theContinuationKey and
         * from any other partition that is co-located with that partition.
         */
        QueryRequest origRequest = rcb.getRequest();
        QueryRequest reqCopy = origRequest.copyInternal();
        reqCopy.setContKey(state.theContinuationKey);

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("ReceiveIter : executing remote request for " +
                      "sorting phase 1");
        }

        QueryResult result = execute(rcb, origRequest, reqCopy);

        int numPids = result.getNumPids();
        List<MapValue> results = result.getResultsInternal();
        state.theIsInSortPhase1 = result.isInPhase1();
        state.theContinuationKey = result.getContinuationKey();

        rcb.tallyReadKB(result.getReadKB());
        rcb.tallyReadUnits(result.getReadUnits());
        rcb.tallyWriteKB(result.getWriteKB());

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("ReceiveIter.initPartitionSort() : got result.\n" +
                      "reached limit = " + result.reachedLimit() +
                      " in phase 1 = " + result.isInPhase1());
        }

        /*
         * For each partition P that was accessed during the execution of
         * the above QueryRequest, collect the results for P and create a
         * scanner that will be used during phase 2 to collect further
         * results from P only.
         */
        int resIdx = 0;

        for (int p = 0; p < numPids; ++p) {

            int pid = result.getPid(p);
            int numResults = result.getNumPartitionResults(p);
            byte[] contKey = result.getPartitionContKey(p);
            assert(numResults > 0);

            ArrayList<MapValue> partitionResults =
                new ArrayList<MapValue>(numResults);

            for (int j = 0; j < numResults; ++j) {

                MapValue res = results.get(resIdx);
                partitionResults.add(res);

                if (rcb.getTraceLevel() >= 1) {
                    rcb.trace("Added result for partition " + pid +
                              ":\n" + res);
                }
                ++resIdx;
            }

            RemoteScanner scanner =
                this.new RemoteScanner(rcb, state, false, pid);
            scanner.addResults(partitionResults, contKey);
            state.theSortedScanners.add(scanner);
        }

        if (rcb.getTraceLevel() >= 1) {
            rcb.trace("ReceiveIter.initPartitionSort() : " +
                      " memory consumption = " + state.theMemoryConsumption);
        }

        /*
         * For simplicity, if the size limit was not reached during this
         * batch of sort phase 1, we don't start a new batch. We let the
         * app do it. Furthermore, this means that each remote fetch will
         * be done with the max amount of read limit, which will reduce the
         * total number of fetches.
         */
        rcb.setReachedLimit(true);
    }

    private void handleTopologyChange(
        RuntimeControlBlock rcb,
        ReceiveIterState state) {

        TopologyInfo newTopoInfo = rcb.getTopologyInfo();

        if (theDistributionKind == DistributionKind.ALL_PARTITIONS ||
            newTopoInfo.equals(state.theTopoInfo)) {
            return;
        }

        int[] newShards = newTopoInfo.getShardIds();
        int[] currShards = state.theTopoInfo.getShardIds();

        for (int i = 0; i < newShards.length; ++i) {

            int j;
            for (j = 0; j < currShards.length; ++j) {
                if (newShards[i] == currShards[j]) {
                    currShards[j] = -1;
                    break;
                }
            }

            if (j < currShards.length) {
                continue;
            }

            /* We have a new shard */
            state.theSortedScanners.add(
                this.new RemoteScanner(rcb, state, true, newShards[i]));
        }

        for (int j = 0; j < currShards.length; ++j) {

            if (currShards[j] == -1) {
                continue;
            }

            /* This shard does not exist any more */
            for (RemoteScanner scanner : state.theSortedScanners) {

                if (scanner.theShardOrPartId == currShards[j]) {
                    state.theSortedScanners.remove(scanner);
                    break;
                }
            }
        }

        state.theTopoInfo = newTopoInfo;
    }

    private boolean checkDuplicate(
        RuntimeControlBlock rcb,
        ReceiveIterState state,
        MapValue res) {

        if (thePrimKeyFields == null) {
            return false;
        }

        BinaryValue binPrimKey = createBinaryPrimKey(res);
        boolean added = state.thePrimKeysSet.add(binPrimKey);
        if (!added) {
            if (rcb.getTraceLevel() >= 1) {
                rcb.trace("ReceiveIter.checkDuplicate() : result was duplicate");
            }
            return true;
        }
        long sz = (binPrimKey.sizeof() + SizeOf.HASHSET_ENTRY_OVERHEAD);
        state.theMemoryConsumption += sz;
        state.theDupElimMemory += sz;
        rcb.incMemoryConsumption(sz);
        return false;
    }

    private BinaryValue createBinaryPrimKey(MapValue result) {

        final ByteArrayOutputStream baos = new ByteArrayOutputStream();
        final DataOutput out = new DataOutputStream(baos);

        try {
            for (int i = 0; i < thePrimKeyFields.length; ++i) {
                FieldValue fval = result.get(thePrimKeyFields[i]);
                writeValue(out, fval, i);
            }
        } catch (IOException e) {
            throw new QueryStateException(
                "Failed to create binary prim key due to IOException:\n" +
                e.getMessage());
        }

        byte[] bytes = baos.toByteArray();
        return new BinaryValue(bytes);
    }

    private void writeValue(DataOutput out, FieldValue val, int i)
        throws IOException {

        switch (val.getType()) {
        case INTEGER:
            SerializationUtil.writePackedInt(out, val.getInt());
            break;
        case LONG:
            SerializationUtil.writePackedLong(out, val.getLong());
            break;
        case DOUBLE:
            out.writeDouble(val.getDouble());
            break;
        case NUMBER:
            NumberValue num = (NumberValue)val;
            SerializationUtil.writeByteArray(out, num.getBytes());
            break;
        case STRING:
            SerializationUtil.writeString(out, val.getString());
            break;
        case TIMESTAMP:
            SerializationUtil.writeString(out, val.getString());
            break;
        default:
            throw new QueryStateException(
                "Unexpected type for primary key column : " +
                val.getType() + ", at result column " + i);
        }
    }

    /**
     * For all-shard, ordering queries, there is one RemoteScanner per shard.
     * In this case, each RemoteScanner will fetch results only from the shard
     * specified by theShardOrPartId.
     *
     * For all-partition, ordering queries, there is one RemoteScanner for
     * each partition that has at least one query result. In this case, each
     * RemoteScanner will fetch results only from the partition specified by
     * theShardOrPartId.
     *
     * For non-ordering queries, there is a single RemoteScanner. It will
     * fetch as many as possible results starting from the shard or partition
     * specified in theContinuationKey (so it may fetch results from more than
     * one shard/partition).
     */
    private class RemoteScanner implements Comparable<RemoteScanner> {

        final RuntimeControlBlock theRCB;

        final ReceiveIterState theState;

        boolean theIsForShard;

        int theShardOrPartId = -1;

        List<MapValue> theResults;

        long theResultsSize;

        int theNextResultPos;

        byte[] theContinuationKey;

        boolean theMoreRemoteResults;

        public RemoteScanner(
            RuntimeControlBlock rcb,
            ReceiveIterState state,
            boolean isForShard,
            int spid) {

            theRCB = rcb;
            theState = state;
            theMoreRemoteResults = true;
            theIsForShard = isForShard;
            theShardOrPartId = spid;
        }

        boolean isDone() {
            return (!theMoreRemoteResults &&
                    (theResults == null ||
                     theNextResultPos >= theResults.size()));
        }

        boolean hasLocalResults() {
            return (theResults != null && theNextResultPos < theResults.size());
        }

        void addResults(List<MapValue> results, byte[] contKey) {
            theResults = results;
            theContinuationKey = contKey;
            theMoreRemoteResults = (contKey != null);
            addMemoryConsumption();
        }

        MapValue nextLocal() {

            if (theResults != null && theNextResultPos < theResults.size()) {
                MapValue res = theResults.get(theNextResultPos);
                theResults.set(theNextResultPos, null);
                ++theNextResultPos;
                return res;
            }

            return null;
        }

        MapValue next() {

            if (theResults != null && theNextResultPos < theResults.size()) {
                return theResults.get(theNextResultPos++);
            }

            theResults = null;
            theNextResultPos = 0;

            if (!theMoreRemoteResults || theRCB.reachedLimit()) {
                return null;
            }

            fetch();

            if (theResults.isEmpty()) {
                return null;
            }

            return theResults.get(theNextResultPos++);
        }

        void fetch() {

            QueryRequest origRequest = theRCB.getRequest();
            QueryRequest reqCopy = origRequest.copyInternal();
            reqCopy.setContKey(theContinuationKey);
            reqCopy.setShardId(theIsForShard ? theShardOrPartId : -1);

            if (doesSort() && !theIsForShard) {
                theState.theMemoryConsumption -= theResultsSize;
                theRCB.decMemoryConsumption(theResultsSize);
                long numResults =
                    ((reqCopy.getMaxMemoryConsumption() - theState.theDupElimMemory) /
                     ((theState.theSortedScanners.size() + 1) *
                      (theState.theTotalResultsSize /
                       theState.theTotalNumResults)));
                if (numResults > 2048) {
                    numResults = 2048;
                }
                reqCopy.setLimit((int)numResults);
            }

            if (theRCB.getTraceLevel() >= 1) {
                theRCB.trace("RemoteScanner : executing remote request. spid = " +
                             theShardOrPartId);
                assert(reqCopy.hasDriver());
            }

            QueryResult result = execute(theRCB, origRequest, reqCopy);

            theResults = result.getResultsInternal();
            theContinuationKey = result.getContinuationKey();
            theNextResultPos = 0;
            theMoreRemoteResults = (theContinuationKey != null);

            theRCB.tallyReadKB(result.getReadKB());
            theRCB.tallyReadUnits(result.getReadUnits());
            theRCB.tallyWriteKB(result.getWriteKB());

            assert(result.reachedLimit() || !theMoreRemoteResults);

            /*
             * For simplicity, if the query is a sorting one, we consider
             * the current batch done as soon as we get the response back
             * from the proxy, even if the batch limit was not reached there.
             */
            if (result.reachedLimit() || doesSort()) {
                theRCB.setReachedLimit(true);
            }

            if (doesSort() && !theIsForShard) {
                addMemoryConsumption();
            }

            if (theRCB.getTraceLevel() >= 1) {
                theRCB.trace("RemoteScanner : got " + theResults.size() +
                             " remote results. More remote resuls = " +
                             theMoreRemoteResults + " reached limit = " +
                             result.reachedLimit() + " read KB = " +
                             result.getReadKB() + " read Units = " +
                             result.getReadUnits() + " write KB = " +
                             result.getWriteKB() + " memory consumption = " +
                             theState.theMemoryConsumption);
            }
        }

        private void addMemoryConsumption() {

                theResultsSize = 0;
                for (int i = 0; i < theResults.size(); ++i) {
                    theResultsSize += theResults.get(i).sizeof();
                }

                theResultsSize += theResults.size() * SizeOf.OBJECT_REF_OVERHEAD;
                theState.theTotalNumResults += theResults.size();
                theState.theTotalResultsSize += theResultsSize;
                theState.theMemoryConsumption += theResultsSize;
                theRCB.incMemoryConsumption(theResultsSize);
        }

        @Override
        public int compareTo(RemoteScanner other) {

            if (!hasLocalResults()) {
                if (!other.hasLocalResults()) {
                    return (theShardOrPartId < other.theShardOrPartId ? -1 : 1);
                }
                return -1;
            }

            if (!other.hasLocalResults()) {
                return 1;
            }

            MapValue v1 = theResults.get(theNextResultPos);
            MapValue v2 = other.theResults.get(other.theNextResultPos);

            int comp =  Compare.sortResults(theRCB, v1, v2,
                                            theSortFields, theSortSpecs);

            if (comp == 0) {
                comp = (theShardOrPartId < other.theShardOrPartId ? -1 : 1);
            }

            return comp;
        }
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append("DistributionKind : ").append(theDistributionKind);
        sb.append(",\n");

        if (theSortFields != null) {
            formatter.indent(sb);
            sb.append("Sort Fields : ");
            for (int i = 0; i < theSortFields.length; ++i) {
                sb.append(theSortFields[i]);
                if (i < theSortFields.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(",\n");
        }

        if (thePrimKeyFields != null) {
            formatter.indent(sb);
            sb.append("Primary Key Fields : ");
            for (int i = 0; i < thePrimKeyFields.length; ++i) {
                sb.append(thePrimKeyFields[i]);
                if (i < thePrimKeyFields.length - 1) {
                    sb.append(", ");
                }
            }
            sb.append(",\n");
        }
    }
}
