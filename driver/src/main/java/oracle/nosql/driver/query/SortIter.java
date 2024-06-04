/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.PriorityQueue;

import oracle.nosql.driver.query.PlanIterState.StateEnum;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;
import oracle.nosql.driver.util.SizeOf;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;

/**
 * Sorts MapValues based on their values on a specified set of top-level
 * fields. It is used by the driver to implement the geo_near function,
 * which sorts results by distance.
 */
public class SortIter extends PlanIter {

    private class CompareFunction implements Comparator<MapValue> {

        RuntimeControlBlock theRCB;

        CompareFunction(RuntimeControlBlock rcb) {
            theRCB = rcb;
        }

        @Override
        public int compare(MapValue v1, MapValue v2) {

            return Compare.sortResults(theRCB,
                                       v1,
                                       v2,
                                       theSortFields,
                                       theSortSpecs);
        }
    }

    private static class ReverseCompareFunction implements Comparator<MapValue> {

        CompareFunction theComparator;

        ReverseCompareFunction(CompareFunction comparator) {
            theComparator = comparator;
        }

        @Override
        public int compare(MapValue v1, MapValue v2) {
            return -theComparator.compare(v1, v2);
        }
    }

    private static class SortIterState extends PlanIterState {

        CompareFunction theComparator;

        int theLimit = -1;

        ArrayList<MapValue> theResults;

        PriorityQueue<MapValue> theResultsQueue;

        MapValue[] theResultsArray;

        int theNumResults;

        int theCurrResult;

        public SortIterState(RuntimeControlBlock rcb, SortIter iter, int limit) {
            super();
            theLimit = limit;
            if (theLimit > 0) {
                theResults = new ArrayList<MapValue>(theLimit);
            } else {
                theResults = new ArrayList<MapValue>(4096);
            }
            theComparator = iter.new CompareFunction(rcb);
        }

        @Override
        public void done() {
            super.done();
            theResults = null;
            theResultsQueue = null;
            theResultsArray = null;
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theCurrResult = 0;
            theNumResults = 0;
            if (theLimit > 0) {
                theResults = new ArrayList<MapValue>(theLimit);
            } else {
                theResults = new ArrayList<MapValue>(4096);
            }
            theResultsQueue = null;
            theResultsArray = null;
        }

        @Override
        public void close() {
            super.close();
            theResults = null;
            theResultsQueue = null;
            theResultsArray = null;
        }
    }

    private final PlanIter theInput;

    private final String[] theSortFields;

    private final SortSpec[] theSortSpecs;

    private final PlanIter theLimit;

    private final boolean theCountMemory;

    public SortIter(ByteInputStream in, PlanIterKind kind, short queryVersion)
        throws IOException {

        super(in, queryVersion);
        theInput = deserializeIter(in, queryVersion);

        theSortFields = SerializationUtil.readStringArray(in);
        theSortSpecs = readSortSpecs(in);

        if (kind == PlanIterKind.SORT2) {
            theCountMemory = in.readBoolean();
        } else {
            theCountMemory = true;
        }

        if (queryVersion >= QueryDriver.QUERY_V5) {
            theLimit = deserializeIter(in, queryVersion);
        } else {
            theLimit = null;
        }
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.SORT;
    }

    @Override
    PlanIter getInputIter() {
        return theInput;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.trace("Opening SortIter");

        int limit = - 1;

        if (theLimit != null) {
            theLimit.open(rcb);
            theLimit.next(rcb);
            FieldValue val = rcb.getRegVal(theLimit.getResultReg());
            limit = val.getInt();
        }

        SortIterState state = new SortIterState(rcb, this, limit);
        rcb.setState(theStatePos, state);
        theInput.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theInput.reset(rcb);
        SortIterState state = (SortIterState)rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInput.close(rcb);
        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        SortIterState state = (SortIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (state.isOpen()) {

            boolean more = theInput.next(rcb);

            while (more) {
                MapValue v = (MapValue)rcb.getRegVal(theInput.getResultReg());

                for (String field : theSortFields) {
                    FieldValue fval = v.get(field);
                    if (!fval.isAtomic() && !fval.isNull()) {
                        throw new QueryException(
                            "Sort expression does not return a single " +
                            "atomic value", theLocation);
                    }
                }

                if (state.theLimit < 0) {
                    state.theResults.add(v);

                    if (theCountMemory) {
                        long sz = v.sizeof() + SizeOf.OBJECT_REF_OVERHEAD;
                        rcb.incMemoryConsumption(sz);
                    }
                } else if (state.theResults != null &&
                           state.theResults.size() < state.theLimit) {

                    state.theResults.add(v);

                    if (theCountMemory) {
                        long sz = v.sizeof() + SizeOf.OBJECT_REF_OVERHEAD;
                        rcb.incMemoryConsumption(sz);
                    }

                    if (state.theResults.size() == state.theLimit) {

                        ReverseCompareFunction comparator =
                        new ReverseCompareFunction(state.theComparator);

                        state.theResultsQueue = new PriorityQueue<>(comparator);

                        for (MapValue result : state.theResults) {
                            state.theResultsQueue.add(result);
                        }
                        state.theResults = null;
                    }
                } else {
                    MapValue lastResult = state.theResultsQueue.peek();

                    if (state.theComparator.compare(lastResult, v) > 0) {

                        state.theResultsQueue.remove();
                        state.theResultsQueue.add(v);

                        if (rcb.getTraceLevel() >= 4) {
                            rcb.trace("SortIter: added top result: " + v +
                                      "\nin place of " + lastResult);
                        }

                        if (theCountMemory) {
                            long sz = v.sizeof();
                            rcb.incMemoryConsumption(sz);
                            sz = lastResult.sizeof();
                            rcb.decMemoryConsumption(sz);
                        }
                    }
                }

                more = theInput.next(rcb);
            }

            if (rcb.reachedLimit()) {
                return false;
            }

            if (state.theResultsQueue != null) {
                /* Move the results from the queue to an array, maintaining their
                 * sort order. This is needed because iterating over the queue
                 * using an Iterator does not guarantee that the results will be
                 * retrieved in their sorted order. */
                state.theResultsArray = new MapValue[state.theLimit];

                int i = state.theLimit - 1;
                while (!state.theResultsQueue.isEmpty()) {
                    MapValue result = state.theResultsQueue.remove();
                    state.theResultsArray[i] = result;
                    --i;
                }

                state.theResultsQueue = null;
                state.theNumResults = state.theLimit;
            } else {
                state.theResults.sort(state.theComparator);
                state.theNumResults = state.theResults.size();
            }

            state.setState(StateEnum.RUNNING);
        }

        if (state.theCurrResult < state.theNumResults) {

            MapValue v;
            if (state.theResultsArray == null) {
                v = state.theResults.get(state.theCurrResult);
                state.theResults.set(state.theCurrResult, null);
            } else {
                v = state.theResultsArray[state.theCurrResult]; 
                state.theResultsArray[state.theCurrResult] = null;
            }

            v.convertEmptyToNull();
            rcb.setRegVal(theResultReg, v);
            ++state.theCurrResult;
            return true;
        }

        state.done();
        return false;
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        theInput.display(sb, formatter);

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
}
