/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.util.ArrayList;
import java.util.concurrent.TimeoutException;

import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.RequestTimeoutException;
import oracle.nosql.driver.RetryableException;
import oracle.nosql.driver.http.Client;
import oracle.nosql.driver.ops.PreparedStatement;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;

/**
 * Drives the execution of "advanced" queries at the driver and contains all
 * the dynamic state needed for this execution. The state is preserved across
 * the query requests submitted by the application (i.e., across batches).
 * @hidden
 */
public class QueryDriver {

    public static short QUERY_V2 = 2;

    public static short QUERY_V3 = 3;

    /* added query name in QueryRequest */
    public static short QUERY_V4 = 4;

    /* Changed VirtualScan info exchanged between sdk and proxy */
    public static final short QUERY_V5 = 5;

    public static final short QUERY_VERSION = QUERY_V5;

    private static final int BATCH_SIZE = 100;

    private static final byte[] DUMMY_CONT_KEY = new byte[1];

    private Client theClient;

    private final QueryRequest theRequest;

    private byte[] theContinuationKey;

    private int thePrepCost;

    private RuntimeControlBlock theRCB;

    /*
     * The max number of results the app will receive per NoSQLHandle.query()
     * invocation
     */
    private final int theBatchSize;

    private ArrayList<MapValue> theResults;

    private NoSQLException theError;

    public QueryDriver(QueryRequest req) {
        theRequest = req;
        req.setDriver(this);
        theBatchSize = (req.getLimit() > 0 ? req.getLimit() : BATCH_SIZE);
    }

    public void setClient(Client client) {
        theClient = client;
    }

    Client getClient() {
        return theClient;
    }

    QueryRequest getRequest() {
        return theRequest;
    }

    public RuntimeControlBlock getRCB() {
        return theRCB;
    }

    public void setPrepCost(int cost) {
        thePrepCost = cost;
    }

    /**
     * Computes a batch of results and fills-in the given QueryResult.
     */
    public void compute(QueryResult result) {

        PreparedStatement prep = theRequest.getPreparedStatement();

        assert(!prep.isSimpleQuery());
        assert(theRequest.getDriver() == this);

        /*
         * If non-null, theError stores a non-retriable exception thrown
         * during a previous batch. In this case, we just rethrow that
         * exception.
         */
        if (theError != null) {
            throw theError;
        }

        Client.trace("QueryDriver: starting batch computation", 2);

        /*
         * theResults may be non-empty if a retryable exception was thrown
         * during a previous batch. In this case, theResults stores the results
         * computed before the exception was thrown, and in this batch we just
         * return what we have.
         */
        if (theResults != null) {
            setQueryResult(result);
            return;
        }

        PlanIter iter = prep.driverPlan();

        if (theRCB == null) {

            theRCB = new RuntimeControlBlock(this,
                                             iter,
                                             prep.numIterators(),
                                             prep.numRegisters(),
                                             prep.getVariableValues());
            /* Tally the compilation cost */
            theRCB.tallyReadKB(thePrepCost);
            theRCB.tallyReadUnits(thePrepCost);

            iter.open(theRCB);
        }

        int i = 0;
        boolean more;
        theResults = new ArrayList<MapValue>(theBatchSize);

        try {
            more = iter.next(theRCB);

            while (more) {

                FieldValue res = theRCB.getRegVal(iter.getResultReg());

                if (!(res instanceof MapValue)) {
                    throw new IllegalStateException(
                        "Query result is not a MapValue:\n" + res);
                }

                theResults.add((MapValue)res);

                if (theRCB.getTraceLevel() >= 2) {
                    theRCB.trace("QueryDriver: got result : " + res);
                }

                ++i;
                if (i == theBatchSize) {
                    break;
                }

                more = iter.next(theRCB);
            }
        } catch (Throwable e) {
            /*
             * If it's not a retryable exception, save it so that we throw it
             * again if the app resubmits the QueryRequest.
             */
            if (!isRetryableException(e)) {
                theError = new NoSQLException(
                    "QueryRequest cannot be continued after throwing a " +
                    "non-retryable exception in a previous execution. " +
                    "Set the continuation key to null in order to execute " +
                    "the query from the beginning", e);
                iter.close(theRCB);
                theResults.clear();
                theResults = null;
            }
            throw e;
        }

        if (!more) {
            if (theRCB.reachedLimit()) {
                theContinuationKey = DUMMY_CONT_KEY;
                theRCB.setReachedLimit(false);
            } else {
                assert(iter.isDone(theRCB));
                theContinuationKey = null;
            }
        } else {
            theContinuationKey = DUMMY_CONT_KEY;
        }

        setQueryResult(result);

        theRequest.setContKey(theContinuationKey);
    }

    private boolean isRetryableException(Throwable e) {
        if (e instanceof RetryableException) {
            return true;
        }
        /*
         * if we got a timeout, and the query does not do writes, allow
         * retrying
         */
       if (((e instanceof TimeoutException) ||
           (e instanceof RequestTimeoutException)) &&
           theRequest != null && theRequest.getPreparedStatement() != null &&
           theRequest.getPreparedStatement().doesWrites() == false) {
           return true;
       }
       return false;
    }

    private void setQueryResult(QueryResult result) {

        result.setResults(theResults);
        result.setContinuationKey(theContinuationKey);
        result.setReadKB(theRCB.getReadKB());
        result.setReadUnits(theRCB.getReadUnits());
        result.setWriteKB(theRCB.getWriteKB());
        result.setRateLimitDelayedMs(theRCB.getRateLimitDelayedMs());
        result.setRetryStats(theRCB.getRetryStats());

        theResults = null;
        theRCB.resetKBConsumption();
    }

    public void close() {
        /* if there is no RCB there is no state to clean up */
        if (theRCB != null) {
            theRequest.getPreparedStatement().driverPlan().close(theRCB);
        }
        if (theResults != null) {
            theResults.clear();
            theResults = null;
        }
    }

    public String getQueryTrace() {
        return theRCB != null ? theRCB.getQueryTrace() : null;
    }
}
