/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;

import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;

/**
 * SFWIter is used for:
 * (a) project out result columns that do not appear in the SELECT list of
 *     the query, but are included in the results fetched from the proxy,
 *     because the are order-by columns or primary-key columns used for
 *     duplicate elimination.
 * (b) For group-by and aggregation queries, regroup and reaggregate the
 *     partial groups/aggregates received from the proxy.
 * (c) implement offset and limit.
 */
public class SFWIter extends PlanIter {

    public static class SFWIterState extends PlanIterState {

        private long theOffset;

        private long theLimit;

        private long theNumResults;

        private FieldValue[] theGBTuple;

        private boolean theHaveGBTuple;

        SFWIterState(SFWIter iter) {
            theGBTuple = new FieldValue[iter.theColumnIters.length];
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theNumResults = 0;
            theHaveGBTuple = false;
        }
    }

    private final PlanIter theFromIter;

    private final String theFromVarName;

    private final PlanIter[] theColumnIters;

    private final String[] theColumnNames;

    private boolean theIsSelectStar;

    private final int theNumGBColumns;

    private final PlanIter theOffsetIter;

    private final PlanIter theLimitIter;

    SFWIter(ByteInputStream in, short queryVersion) throws IOException {

        super(in, queryVersion);
        theColumnNames = SerializationUtil.readStringArray(in);
        theNumGBColumns = in.readInt();
        theFromVarName = SerializationUtil.readString(in);
        theIsSelectStar = in.readBoolean();
        theColumnIters = deserializeIters(in, queryVersion);
        theFromIter = deserializeIter(in, queryVersion);
        theOffsetIter = deserializeIter(in, queryVersion);
        theLimitIter = deserializeIter(in, queryVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.SFW;
    }

    @Override
    PlanIter getInputIter() {
        return theFromIter;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        SFWIterState state = new SFWIterState(this);

        rcb.setState(theStatePos, state);

        theFromIter.open(rcb);

        for (PlanIter columnIter : theColumnIters) {
            columnIter.open(rcb);
        }

        computeOffsetLimit(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        SFWIterState state = (SFWIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (state.theNumResults >= state.theLimit) {
            state.done();
            return false;
        }

        /* while loop for skipping offset results */
        while (true) {

            boolean more = computeNextResult(rcb, state);

            if (!more) {
                return false;
            }

            /*
             * Even though we have a result, the state may be DONE. This is the
             * case when the result is the last group tuple in a grouping SFW.
             * In this case, if we have not reached the offset yet, we should
             * ignore this result and return false.
             */
            if (state.isDone() && state.theOffset > 0) {
                return false;
            }

            if (state.theOffset == 0) {
                ++state.theNumResults;
                break;
            }

            --state.theOffset;
        }

        return true;
    }

    boolean computeNextResult(RuntimeControlBlock rcb, SFWIterState state) {

        /* while loop for group by */
        while (true) {

            boolean more = theFromIter.next(rcb);

            if (!more) {

                if (!rcb.reachedLimit()) {
                    state.done();
                }

                if (theNumGBColumns >= 0) {
                    return produceLastGroup(rcb, state);
                }

                return false;
            }

            /*
             * Compute the exprs in the SELECT list. If this is a grouping
             * SFW, compute only the group-by columns. However, skip this
             * computation if this is not a grouping SFW and it has an offset
             * that has not been reached yet.
             */

            if (theNumGBColumns < 0 && state.theOffset > 0) {
                return true;
            }

            int numCols = (theNumGBColumns >= 0 ?
                           theNumGBColumns :
                           theColumnIters.length);
            int i = 0;

            for (i = 0; i < numCols; ++i) {

                PlanIter columnIter = theColumnIters[i];
                more = columnIter.next(rcb);

                if (!more) {

                    if (theNumGBColumns > 0) {
                        columnIter.reset(rcb);
                        break;
                    }

                    rcb.setRegVal(columnIter.getResultReg(),
                                  NullValue.getInstance());
                } else {
                    if (rcb.getTraceLevel() >= 3) {
                        rcb.trace("SFW: Value for SFW column " + i + " = " +
                                  rcb.getRegVal(columnIter.getResultReg()));
                    }
                }

                columnIter.reset(rcb);
            }

            if (i < numCols) {
                continue;
            }

            if (theNumGBColumns < 0) {

                if (theIsSelectStar) {
                    break;
                }

                MapValue result = new MapValue();
                rcb.setRegVal(theResultReg, result);

                for (i = 0; i < theColumnIters.length; ++i) {
                    PlanIter columnIter = theColumnIters[i];
                    FieldValue value = rcb.getRegVal(columnIter.getResultReg());
                    result.put(theColumnNames[i], value);
                }
                break;
            }

            if (groupInputTuple(rcb, state)) {
                break;
            }
        }

        return true;
    }

    /*
     * This method checks whether the current input tuple (a) starts the
     * first group, i.e. it is the very 1st tuple in the input stream, or
     * (b) belongs to the current group, or (c) starts a new group otherwise.
     * The method returns true in case (c), indicating that an output tuple
     * is ready to be returned to the consumer of this SFW. Otherwise, false
     * is returned.
     */
    boolean groupInputTuple(RuntimeControlBlock rcb, SFWIterState state) {

        int numCols = theColumnIters.length;

        /*
         * If this is the very first input tuple, start the first group and
         * go back to compute next input tuple.
         */
        if (!state.theHaveGBTuple) {

            for (int i = 0; i < theNumGBColumns; ++i) {
                state.theGBTuple[i] = rcb.getRegVal(
                    theColumnIters[i].getResultReg());
            }

            for (int i = theNumGBColumns; i < numCols; ++i) {
                theColumnIters[i].next(rcb);
                theColumnIters[i].reset(rcb);
            }

            state.theHaveGBTuple = true;

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("SFW: Started first group:");
                traceCurrentGroup(rcb, state);
            }

            return false;
        }

        /*
         * Compare the current input tuple with the current group tuple.
         */
        int j;
        for (j = 0; j < theNumGBColumns; ++j) {
            FieldValue newval = rcb.getRegVal(theColumnIters[j].getResultReg());
            FieldValue curval = state.theGBTuple[j];
            if (!newval.equals(curval)) {
                break;
            }
        }

        /*
         * If the input tuple is in current group, update the aggregate
         * functions and go back to compute the next input tuple.
         */
        if (j == theNumGBColumns) {

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("SFW: Input tuple belongs to current group:");
                traceCurrentGroup(rcb, state);
            }

            for (int i = theNumGBColumns; i < numCols; ++i) {
                theColumnIters[i].next(rcb);
                theColumnIters[i].reset(rcb);
            }

            return false;
        }

        /*
         * Input tuple starts new group. We must finish up the current group,
         * produce a result (output tuple) from it, and init the new group.
         */

        // 1. Get the final aggregate values for the current group and store
        //    them in theGBTuple.
        for (int i = theNumGBColumns; i < numCols; ++i) {
            state.theGBTuple[i] = theColumnIters[i].getAggrValue(rcb, true);
        }

        // 2. Create a result MapValue out of the GB tuple
        MapValue result = new MapValue();
        rcb.setRegVal(theResultReg, result);

        for (int i = 0; i < theColumnIters.length; ++i) {
            result.put(theColumnNames[i], state.theGBTuple[i]);
        }

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("SFW: Current group done: " + result);
        }

        // 3. Put the values of the grouping columns into the GB tuple
        for (int i = 0; i < theNumGBColumns; ++i) {
            PlanIter columnIter = theColumnIters[i];
            state.theGBTuple[i] = rcb.getRegVal(columnIter.getResultReg());
        }

        // 4. Compute the values of the aggregate functions.
        for (int i = theNumGBColumns; i < numCols; ++i) {
            theColumnIters[i].next(rcb);
            theColumnIters[i].reset(rcb);
        }

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("SFW: Started new group:");
            traceCurrentGroup(rcb, state);
        }

        return true;
    }

    boolean produceLastGroup(RuntimeControlBlock rcb, SFWIterState state) {

        if (rcb.reachedLimit()) {
            return false;
        }

        /*
         * If there is no group, return false.
         */
        if (!state.theHaveGBTuple) {
            return false;
        }

        MapValue result = new MapValue();
        rcb.setRegVal(theResultReg, result);

        for (int i = 0; i < theNumGBColumns; ++i) {
            result.put(theColumnNames[i], state.theGBTuple[i]);
        }

        for (int i = theNumGBColumns; i < theColumnIters.length; ++i) {
            result.put(theColumnNames[i],
                       theColumnIters[i].getAggrValue(rcb, true));
        }

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("SFW: Produced last group : " + result);
        }

        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theFromIter.reset(rcb);

        for (PlanIter columnIter : theColumnIters) {
            columnIter.reset(rcb);
        }

        if (theOffsetIter != null) {
            theOffsetIter.reset(rcb);
        }

        if (theLimitIter != null) {
            theLimitIter.reset(rcb);
        }

        SFWIterState state = (SFWIterState)rcb.getState(theStatePos);
        state.reset(this);

        computeOffsetLimit(rcb);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        SFWIterState state = (SFWIterState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theFromIter.close(rcb);

        for (PlanIter columnIter : theColumnIters) {
            columnIter.close(rcb);
        }

        if (theOffsetIter != null) {
            theOffsetIter.close(rcb);
        }

        if (theLimitIter != null) {
            theLimitIter.close(rcb);
        }

        state.close();
    }

    private void computeOffsetLimit(RuntimeControlBlock rcb) {

        SFWIterState state = (SFWIterState)rcb.getState(theStatePos);

        long offset = 0;
        long limit = -1;

        if (theOffsetIter != null) {
            theOffsetIter.open(rcb);
            theOffsetIter.next(rcb);
            FieldValue val = rcb.getRegVal(theOffsetIter.getResultReg());
            offset = val.getLong();

            if (offset < 0) {
                throw new QueryException(
                   "Offset can not be a negative number",
                    theOffsetIter.theLocation);
            }

            if (offset > Integer.MAX_VALUE) {
                throw new QueryException(
                   "Offset can not be greater than Integer.MAX_VALUE",
                    theOffsetIter.theLocation);
            }
        }

        if (theLimitIter != null) {
            theLimitIter.open(rcb);
            theLimitIter.next(rcb);
            FieldValue val = rcb.getRegVal(theLimitIter.getResultReg());
            limit = val.getLong();

            if (limit < 0) {
                throw new QueryException(
                    "Limit can not be a negative number",
                    theLimitIter.theLocation);
            }

            if (limit > Integer.MAX_VALUE) {
                throw new QueryException(
                   "Limit can not be greater than Integer.MAX_VALUE",
                    theOffsetIter.theLocation);
            }
        }

        if (limit < 0) {
            limit = Long.MAX_VALUE;
        }

        state.theOffset = offset;
        state.theLimit = limit;
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append("FROM:\n");
        theFromIter.display(sb, formatter);
        sb.append(" as");
        sb.append(" " + theFromVarName);
        sb.append("\n\n");

        if (theNumGBColumns >= 0) {
            formatter.indent(sb);
            sb.append("GROUP BY:\n");
            formatter.indent(sb);
            if (theNumGBColumns == 0) {
                sb.append("No grouping expressions");
            } else if (theNumGBColumns == 1) {
                sb.append(
                    "Grouping by the first expression in the SELECT list");
            } else {
                sb.append("Grouping by the first " + theNumGBColumns +
                          " expressions in the SELECT list");
            }
            sb.append("\n\n");
        }

        formatter.indent(sb);
        sb.append("SELECT:\n");

        for (int i = 0; i < theColumnIters.length; ++i) {
            theColumnIters[i].display(sb, formatter);
            if (i < theColumnIters.length - 1) {
                sb.append(",\n");
            }
        }

        if (theOffsetIter != null) {
            sb.append("\n\n");
            formatter.indent(sb);
            sb.append("OFFSET:\n");
            theOffsetIter.display(sb, formatter);
        }

        if (theLimitIter != null) {
            sb.append("\n\n");
            formatter.indent(sb);
            sb.append("LIMIT:\n");
            theLimitIter.display(sb, formatter);
        }
    }

    void traceCurrentGroup(RuntimeControlBlock rcb, SFWIterState state) {

        for (int i = 0; i < theNumGBColumns; ++i) {
            rcb.trace("SFW: Val " + i + " = " + state.theGBTuple[i]);
        }

        for (int i = theNumGBColumns; i < theColumnIters.length; ++i) {
            rcb.trace("SFW: Val " + i + " = " +
                      theColumnIters[i].getAggrValue(rcb, false));
        }
    }
}
