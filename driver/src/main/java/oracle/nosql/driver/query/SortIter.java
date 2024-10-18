/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;
import java.util.ArrayList;

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

    private static class SortIterState extends PlanIterState {

        final ArrayList<MapValue> theResults;

        int theCurrResult;

        public SortIterState() {
            super();
            theResults = new ArrayList<MapValue>(128);
        }

        @Override
        public void done() {
            super.done();
            theCurrResult = 0;
            theResults.clear();
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theCurrResult = 0;
            theResults.clear();
        }

        @Override
        public void close() {
            super.close();
            theResults.clear();
        }
    }

    private final PlanIter theInput;

    private final String[] theSortFields;

    private final SortSpec[] theSortSpecs;

    private final boolean theCountMemory;

    public SortIter(ByteInputStream in, PlanIterKind kind, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        theInput = deserializeIter(in, serialVersion);

        theSortFields = SerializationUtil.readStringArray(in);
        theSortSpecs = readSortSpecs(in);

        if (kind == PlanIterKind.SORT2) {
            theCountMemory = in.readBoolean();
        } else {
            theCountMemory = true;
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
        SortIterState state = new SortIterState();
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

                state.theResults.add(v);

                if (theCountMemory) {
                    long sz = v.sizeof() + SizeOf.OBJECT_REF_OVERHEAD;
                    rcb.incMemoryConsumption(sz);
                }

                more = theInput.next(rcb);
            }

            if (rcb.reachedLimit()) {
                return false;
            }

            state.theResults.sort(
                    (v1, v2) -> Compare.sortResults(rcb,
                                               v1,
                                               v2,
                                               theSortFields,
                                               theSortSpecs));

            state.setState(StateEnum.RUNNING);
        }

        if (state.theCurrResult < state.theResults.size()) {

            MapValue v = state.theResults.get(state.theCurrResult);
            v.convertEmptyToNull();
            rcb.setRegVal(theResultReg, v);
            state.theResults.set(state.theCurrResult, null);
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
