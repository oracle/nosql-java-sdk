/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.math.MathContext;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.http.Client;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.RetryStats;
import oracle.nosql.driver.values.FieldValue;

/**
 * Stores all state of an executing query plan. There is a single RCB instance
 * per query execution, and all iterators have access to that instance during
 * te execution.
 */
public class RuntimeControlBlock {

    /*
     * The QueryDriver acts as the query coordinator and gives access to the
     * Client, the QueryRequest, and the PreparedStatement.
     */
    private final QueryDriver theQueryDriver;

    /*
     * An array storing the values of the extenrnal variables set for the
     * operation. These come from the map in the BoundStatement.
     */
    private final FieldValue[] theExternalVars;

    private final PlanIter theRootIter;

    /*
     * The state array contains as many elements as there are PlanIter instances
     * in the query plan to be executed.
     */
    private final PlanIterState[] theIteratorStates;

    /*
     * The register array contains as many elements as required by the
     * instances in the query plan to be executed.
     */
    private final FieldValue[] theRegisters;

    /*
     * Indicates if the query execution reached the size-based or number-based
     * limit. If so, query execution must stop and a batch of results
     * (potentially empty) must be returned to the app.
     */
    private boolean theReachedLimit;

    /*
     * The total readKB/writeKB that were consumed during the execution of
     * one query batch.
     */
    private int theReadKB;
    private int theReadUnits;
    private int theWriteKB;

    /*
     * Stats for retries and rate limiting during query batch execution
     */
    private int theRateLimitDelayedMs;
    private RetryStats theRetryStats;

    /*
     * The number of memory bytes consumed by the query at the client for
     * blocking operations (duplicate eleimination, sorting). Not applicable
     * to the server RCBs.
     */
    long theMemoryConsumption;

    public RuntimeControlBlock(
        QueryDriver driver,
        PlanIter rootIter,
        int numIters,
        int numRegs,
        FieldValue[] externalVars) {

        theQueryDriver = driver;

        theRootIter = rootIter;

        theIteratorStates = new PlanIterState[numIters];
        theRegisters = new FieldValue[numRegs];
        theExternalVars = externalVars;
    }

    public int getTraceLevel() {
        return getRequest().getTraceLevel();
    }

    public void trace(String msg) {
        /* TODO: think about logging */
        System.out.println("D-QUERY: " + msg);
    }

    public Client getClient() {
        return theQueryDriver.getClient();
    }

    public QueryRequest getRequest() {
        return theQueryDriver.getRequest();
    }

    TopologyInfo getTopologyInfo() {
        return theQueryDriver.getTopologyInfo();
    }

    Consistency getConsistency() {
        return getRequest().getConsistency();
    }

    long getTimeout() {
        return getRequest().getTimeout();
    }

    public int getMaxReadKB() {
        return getRequest().getMaxReadKB();
    }

    public MathContext getMathContext() {
        return getRequest().getMathContext();
    }

    public long getMaxMemoryConsumption() {
        return getRequest().getMaxMemoryConsumption();
    }

    void incMemoryConsumption(long v) {

        theMemoryConsumption += v;
        assert(theMemoryConsumption >= 0);

        if (theMemoryConsumption > getMaxMemoryConsumption()) {
            throw new QueryStateException(
                "Memory consumption at the client exceeded maximum " +
                "allowed value " + getMaxMemoryConsumption());
        }
    }

    void decMemoryConsumption(long v) {
        theMemoryConsumption -= v;
        assert(theMemoryConsumption >= 0);
    }

    FieldValue[] getExternalVars() {
        return theExternalVars;
    }

    FieldValue getExternalVar(int id) {

        if (theExternalVars == null) {
            return null;
        }
        return theExternalVars[id];
    }

    PlanIter getRootIter() {
        return theRootIter;
    }

    public void setState(int pos, PlanIterState state) {
        theIteratorStates[pos] = state;
    }

    public PlanIterState getState(int pos) {
        return theIteratorStates[pos];
    }

    public FieldValue[] getRegisters() {
        return theRegisters;
    }

    public FieldValue getRegVal(int regId) {
        return theRegisters[regId];
    }

    public void setRegVal(int regId, FieldValue value) {
        theRegisters[regId] = value;
    }

    public void tallyRateLimitDelayedMs(int ms) {
        theRateLimitDelayedMs += ms;
    }

    public void tallyRetryStats(RetryStats rs) {
        if (rs == null) {
            return;
        }
        if (theRetryStats == null) {
            theRetryStats = new RetryStats();
        }
        theRetryStats.addStats(rs);
    }

    public void tallyReadKB(int nkb) {
        theReadKB += nkb;
    }

    public void tallyReadUnits(int nkb) {
        theReadUnits += nkb;
    }

    public void tallyWriteKB(int nkb) {
        theWriteKB += nkb;
    }

    public int getReadKB() {
        return theReadKB;
    }

    public int getReadUnits() {
        return theReadUnits;
    }

    public void resetKBConsumption() {
        theReadKB = 0;
        theReadUnits = 0;
        theWriteKB = 0;
        theRateLimitDelayedMs = 0;
        theRetryStats = null;
    }

    public int getWriteKB() {
        return theWriteKB;
    }

    public RetryStats getRetryStats() {
        return theRetryStats;
    }

    public int getRateLimitDelayedMs() {
        return theRateLimitDelayedMs;
    }

    public void setReachedLimit(boolean value) {
        theReachedLimit = value;
    }

    public boolean reachedLimit() {
        return theReachedLimit;
    }
}
