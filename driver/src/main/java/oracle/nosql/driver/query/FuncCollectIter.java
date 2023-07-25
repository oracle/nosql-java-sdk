/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;

import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SizeOf;

public class FuncCollectIter extends PlanIter {

    static class CompareFunction implements Comparator<FieldValue> {

        final RuntimeControlBlock theRCB;
        final SortSpec theSortSpec;

        CompareFunction(RuntimeControlBlock rcb) {

            theRCB = rcb;
            theSortSpec = new SortSpec();
        }

        @Override
        public int compare(FieldValue v1, FieldValue v2) {

            return Compare.compareTotalOrder(theRCB, v1, v2, theSortSpec);
        }
    }

    static class WrappedValue {

        public FieldValue theValue;

        WrappedValue(FieldValue value) {
            theValue = value;
        }

        @Override
        public boolean equals(Object other) {
            WrappedValue o = (WrappedValue)other;
            return Compare.equal(this.theValue, o.theValue);
        }

        @Override
        public int hashCode() {
            return Compare.hashcode(theValue);
        }

        public long sizeof() {
            return SizeOf.OBJECT_OVERHEAD +
                   SizeOf.OBJECT_REF_OVERHEAD +
                   theValue.sizeof();
        }
    }

    private class CollectIterState extends PlanIterState {

        final CompareFunction theComparator;

        ArrayValue theArray;

        HashSet<WrappedValue> theValues;

        long theMemoryConsumption;

        RuntimeControlBlock theRCB;

        CollectIterState(RuntimeControlBlock rcb) {

            super();
            theComparator = new CompareFunction(rcb);
            theArray = new ArrayValue();
            theValues = new HashSet<WrappedValue>(128);
            theRCB = rcb;
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theArray = new ArrayValue();
            theValues.clear();
            theRCB.decMemoryConsumption(theMemoryConsumption);
            theMemoryConsumption = 0;
        }

        @Override
        public void close() {
            super.close();
            theArray = null;
            theValues = null;
        }
    }

    private final boolean theIsDistinct;

    private final PlanIter theInput;

    FuncCollectIter(ByteInputStream in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theIsDistinct = in.readBoolean();
        theInput = deserializeIter(in, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FN_COLLECT;
    }

    @Override
    PlanIter getInputIter() {
        return theInput;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new CollectIterState(rcb));
        theInput.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        theInput.reset(rcb);
        /*
         * Don't reset the state of "this". Resetting the state is done in
         * method getAggrValue below.
         */
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

        CollectIterState state = (CollectIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (true) {

            boolean more = theInput.next(rcb);

            if (!more) {
                return true;
            }

            FieldValue val = rcb.getRegVal(theInput.getResultReg());

            if (rcb.getTraceLevel() >= 2) {
                rcb.trace("Collecting value " + val);
            }

            aggregate(rcb, val);
        }
    }

    void aggregate(RuntimeControlBlock rcb, FieldValue val) {

        CollectIterState state = (CollectIterState)rcb.getState(theStatePos);

        if (val.isNull() || val.isEMPTY()) {
            return;
        }

        if (theIsDistinct) {
            ArrayValue arr = (ArrayValue)val;
            int size = arr.size();
            for (int i = 0; i < size; ++i) {
                WrappedValue wval = new WrappedValue(arr.get(i));
                state.theValues.add(wval);
                long sz = wval.sizeof();
                rcb.incMemoryConsumption(sz);
                state.theMemoryConsumption += sz;
            }
        } else {
            ArrayValue arr = (ArrayValue)val;
            state.theArray.addAll(arr.iterator());
            long sz = arr.sizeof() + arr.size() * SizeOf.OBJECT_REF_OVERHEAD;
            rcb.incMemoryConsumption(sz);
            state.theMemoryConsumption += sz;
        }
    }

    @Override
    FieldValue getAggrValue(RuntimeControlBlock rcb, boolean reset) {

        CollectIterState state = (CollectIterState)rcb.getState(theStatePos);

        ArrayValue res;

        if (theIsDistinct) {
            res = new ArrayValue();
            Iterator<WrappedValue> iter = state.theValues.iterator();
            while (iter.hasNext()) {
                res.add(iter.next().theValue);
            }
        } else {
            res = state.theArray;
        }

        res.sort(state.theComparator);

        if (rcb.getTraceLevel() >= 3) {
            rcb.trace("Collected values " + res);
        }

        if (reset) {
            state.reset(this);
        }
        return res;
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append("\"distinct\" : ").append(theIsDistinct);
        sb.append(",\n");
        theInput.display(sb, formatter);
    }
}
