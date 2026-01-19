/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;

import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.query.FuncCollectIter.CompareFunction;
import oracle.nosql.driver.query.FuncCollectIter.WrappedValue;
import oracle.nosql.driver.util.SizeOf;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;

public class GroupIter extends PlanIter {

    private static class GroupTuple {

        FieldValue[] theValues;

        GroupTuple(int numGBColumns) {
            theValues = new FieldValue[numGBColumns];
        }

        @Override
        public boolean equals(Object other) {

            GroupTuple o = (GroupTuple)other;

            for (int i = 0; i < theValues.length; ++i) {

                if (!Compare.equal(theValues[i], o.theValues[i])) {
                    return false;
                }
            }

            return true;
        }

        @Override
        public int hashCode() {
            int code = 1;
            for (int i = 0; i < theValues.length; ++i) {
                code += 31 * code + Compare.hashcode(theValues[i]);
            }
            return code;
        }

        long sizeof() {

            long size = (SizeOf.OBJECT_OVERHEAD +
                         SizeOf.ARRAY_OVERHEAD +
                         (theValues.length + 1) * SizeOf.OBJECT_REF_OVERHEAD);
            for (FieldValue val : theValues) {
                size += val.sizeof();
            }

            return size;
        }
    }

    private static class AggrValue {

        Object theValue;

        boolean theGotNumericInput;

        boolean theIsRegrouping;

        AggrValue(FuncCode aggrIterKind, boolean isRegrouping) {

            theIsRegrouping = isRegrouping;

            switch (aggrIterKind) {
            case FN_COUNT:
            case FN_COUNT_NUMBERS:
            case FN_COUNT_STAR:
            case FN_SUM:
                theValue = new LongValue(0);
                break;
            case FN_MIN:
            case FN_MAX:
                theValue = NullValue.getInstance();
                break;
            case FN_ARRAY_COLLECT:
                theValue = new ArrayValue();
                break;
            case FN_ARRAY_COLLECT_DISTINCT:
                theValue = new HashSet<WrappedValue>();
                break;
            default:
                assert(false);
            }
        }

        @SuppressWarnings("unchecked")
        long sizeof() {
            long sz = (SizeOf.OBJECT_OVERHEAD + SizeOf.OBJECT_REF_OVERHEAD + 1);
            if (theValue instanceof FieldValue) {
                sz += ((FieldValue)theValue).sizeof();
            } else {
                HashSet<WrappedValue> collectSet =
                    (HashSet<WrappedValue>)theValue;
                Iterator<WrappedValue> iter = collectSet.iterator();
                while (iter.hasNext()) {
                    sz +=
                        (SizeOf.HASHSET_ENTRY_OVERHEAD + iter.next().sizeof());
                }
            }
            return sz;
        }

        @SuppressWarnings("unchecked")
        void collect(
            RuntimeControlBlock rcb,
            FieldValue val,
            boolean countMemory) {

            if (val.isNull() || val.isEMPTY()) {
                return;
            }

            boolean isDistinct = !(theValue instanceof FieldValue);

            if (isDistinct) {
                HashSet<WrappedValue> collectSet =
                    (HashSet<WrappedValue>)theValue;
                ArrayValue arrval = (ArrayValue)val;
                for (FieldValue elem : arrval) {
                    WrappedValue welem = new WrappedValue(elem);
                    collectSet.add(welem);
                    if (countMemory) {
                        rcb.incMemoryConsumption(welem.sizeof() +
                                                 SizeOf.HASHSET_ENTRY_OVERHEAD);
                    }
                }
            } else {
                ArrayValue collectArray = (ArrayValue)theValue;
                if (theIsRegrouping) {
                    ArrayValue arrayVal = (ArrayValue)val;
                    collectArray.addAll(arrayVal.iterator());
                    if (countMemory) {
                        rcb.incMemoryConsumption(val.sizeof() +
                                                 SizeOf.OBJECT_REF_OVERHEAD *
                                                 arrayVal.size());
                    }
                } else {
                    collectArray.add(val);
                    if (countMemory) {
                        rcb.incMemoryConsumption(val.sizeof() +
                                                 SizeOf.OBJECT_REF_OVERHEAD);
                    }
                }
            }
        }

        void add(
            RuntimeControlBlock rcb,
            GroupIterState state,
            boolean countMemory,
            FieldValue val,
            MathContext ctx) {

            BigDecimal bd;
            long sz = 0;
            FieldValue sumValue = (FieldValue)theValue;

            switch (val.getType()) {
            case INTEGER: {
                theGotNumericInput = true;
                switch (sumValue.getType()) {
                case LONG: {
                    long sum = ((LongValue)theValue).getValue();
                    sum += ((IntegerValue)val).getValue();
                    ((LongValue)theValue).setValue(sum);
                    break;
                }
                case DOUBLE: {
                    double sum = ((DoubleValue)theValue).getValue();
                    sum += ((IntegerValue)val).getValue();
                    ((DoubleValue)theValue).setValue(sum);
                    break;
                }
                case NUMBER: {
                    BigDecimal sum = ((NumberValue)theValue).getValue();
                    bd = new BigDecimal(((IntegerValue)val).getValue());
                    sum = sum.add(bd, ctx);
                    ((NumberValue)theValue).setValue(sum);
                    break;
                }
                default:
                    assert(false);
                }
                break;
            }
            case LONG: {
                theGotNumericInput = true;
                switch (sumValue.getType()) {
                case LONG: {
                    long sum = ((LongValue)theValue).getValue();
                    sum += ((LongValue)val).getValue();
                    ((LongValue)theValue).setValue(sum);
                    break;
                }
                case DOUBLE: {
                    double sum = ((DoubleValue)theValue).getValue();
                    sum += ((LongValue)val).getValue();
                    ((DoubleValue)theValue).setValue(sum);
                    break;
                }
                case NUMBER: {
                    BigDecimal sum = ((NumberValue)theValue).getValue();
                    bd = new BigDecimal(((LongValue)val).getValue());
                    sum = sum.add(bd, ctx);
                    ((NumberValue)theValue).setValue(sum);
                    break;
                }
                default:
                    assert(false);
                }
                break;
            }
            case DOUBLE: {
                theGotNumericInput = true;
                switch (sumValue.getType()) {
                case LONG: {
                    double sum = ((LongValue)theValue).getValue();
                    sum += ((DoubleValue)val).getValue();
                    if (countMemory) {
                        sz = sumValue.sizeof();
                    }
                    sumValue = new DoubleValue(sum);
                    theValue = sumValue;
                    if (countMemory) {
                        rcb.incMemoryConsumption(sumValue.sizeof() - sz);
                    }
                    break;
                }
                case DOUBLE: {
                    double sum = ((DoubleValue)theValue).getValue();
                    sum += ((DoubleValue)val).getValue();
                    ((DoubleValue)theValue).setValue(sum);
                    break;
                }
                case NUMBER: {
                    BigDecimal sum = ((NumberValue)theValue).getValue();
                    bd = new BigDecimal(((DoubleValue)val).getValue());
                    sum = sum.add(bd, ctx);
                    ((NumberValue)theValue).setValue(sum);
                    break;
                }
                default:
                    assert(false);
                }
                break;
            }
            case NUMBER: {
                theGotNumericInput = true;
                switch (sumValue.getType()) {
                case LONG: {
                    BigDecimal sum =
                        new BigDecimal(((LongValue)theValue).getValue());
                    sum = sum.add(((NumberValue)val).getValue(), ctx);
                    if (countMemory) {
                        sz = sumValue.sizeof();
                    }
                    sumValue = new NumberValue(sum);
                    theValue = sumValue;
                    if (countMemory) {
                        rcb.incMemoryConsumption(sumValue.sizeof() - sz);
                    }
                    break;
                }
                case DOUBLE: {
                    BigDecimal sum =
                        new BigDecimal(((DoubleValue)theValue).getValue());
                    sum = sum.add(((NumberValue)val).getValue(), ctx);
                    if (countMemory) {
                        sz = sumValue.sizeof();
                    }
                    sumValue = new NumberValue(sum);
                    theValue = sumValue;
                    if (countMemory) {
                        rcb.incMemoryConsumption(sumValue.sizeof() - sz);
                    }
                    break;
                }
                case NUMBER: {
                    BigDecimal sum = ((NumberValue)theValue).getValue();
                    sum = sum.add(((NumberValue)val).getValue(), ctx);
                    ((NumberValue)theValue).setValue(sum);
                    break;
                }
                default:
                    assert(false);
                }
                break;
            }
            default:
                break;
            }
        }
    }

    private static class GroupIterState extends PlanIterState {

        final CompareFunction theComparator;

        final HashMap<GroupTuple, AggrValue[]> theResults;

        Iterator<Map.Entry<GroupTuple, AggrValue[]>> theResultsIter;

        GroupTuple theGBTuple;

        public GroupIterState(RuntimeControlBlock rcb, GroupIter iter) {
            super();
            theComparator = new CompareFunction(rcb);
            theResults = new HashMap<GroupTuple, AggrValue[]>(4096);
            theGBTuple = new GroupTuple(iter.theNumGBColumns);
        }

        @Override
        public void done() {
            super.done();
            theResultsIter = null;
            theResults.clear();
            theGBTuple = null;
        }

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theResultsIter = null;
            theResults.clear();
        }

        @Override
        public void close() {
            super.close();
            theResults.clear();
            theResultsIter = null;
            theGBTuple = null;
        }
    }

    private static final FieldValue one = new LongValue(1);

    private final PlanIter theInput;

    private final int theNumGBColumns;

    private final String[] theColumnNames;

    private final FuncCode[] theAggrFuncs;

    private final boolean theIsDistinct;

    private final boolean theRemoveProducedResult;

    private final boolean theCountMemory;

    private final boolean theIsRegrouping;

    public GroupIter(ByteInputStream in, short serialVersion) throws IOException {

        super(in, serialVersion);

        theInput = deserializeIter(in, serialVersion);
        theNumGBColumns = in.readInt();
        theColumnNames = SerializationUtil.readStringArray(in);

        int numAggrs = theColumnNames.length - theNumGBColumns;

        theAggrFuncs = new FuncCode[numAggrs];

        for (int i = 0; i < numAggrs; ++i) {
            short kvcode = in.readShort();
            theAggrFuncs[i] = FuncCode.valueOf(kvcode);
        }

        theIsDistinct = in.readBoolean();
        theRemoveProducedResult = in.readBoolean();
        theCountMemory = in.readBoolean();
        theIsRegrouping = in.readBoolean();
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.GROUP;
    }

    @Override
    PlanIter getInputIter() {
        return theInput;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        GroupIterState state = new GroupIterState(rcb, this);
        rcb.setState(theStatePos, state);
        theInput.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        GroupIterState state = (GroupIterState)rcb.getState(theStatePos);
        state.reset(this);
        theInput.reset(rcb);
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

        GroupIterState state = (GroupIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        while (true) {
            if (state.theResultsIter != null) {

                if (state.theResultsIter.hasNext()) {
                    Map.Entry<GroupTuple, AggrValue[]> tuple =
                        state.theResultsIter.next();
                    GroupTuple gbTuple = tuple.getKey();
                    AggrValue[] aggrTuple = tuple.getValue();
                    MapValue res = new MapValue();

                    int i;
                    for (i = 0; i < theNumGBColumns; ++i) {
                        res.put(theColumnNames[i], gbTuple.theValues[i]);
                    }
                    for (; i < theColumnNames.length; ++i) {
                        FieldValue aggr = getAggrValue(rcb, state, aggrTuple, i);
                        res.put(theColumnNames[i], aggr);
                    }

                    rcb.setRegVal(theResultReg, res);

                    if (theRemoveProducedResult) {
                        state.theResultsIter.remove();
                    }

                    return true;
                }

                state.done();
                return false;
            }

            boolean more = theInput.next(rcb);

            if (!more) {

                if (rcb.reachedLimit()) {
                    return false;
                }

                if (theNumGBColumns == theColumnNames.length) {
                    state.done();
                    return false;
                }

                state.theResultsIter = state.theResults.entrySet().iterator();
                continue;
            }

            int i;
            MapValue inTuple = (MapValue)rcb.getRegVal(theInput.getResultReg());

            for (i = 0; i < theNumGBColumns; ++i) {
                FieldValue colValue = inTuple.get(theColumnNames[i]);
                if (colValue.isEMPTY()) {
                    if (theIsDistinct) {
                        colValue = NullValue.getInstance();
                    } else {
                        break;
                    }
                }
                state.theGBTuple.theValues[i] = colValue;
            }

            if (i < theNumGBColumns) {
                continue;
            }

            AggrValue[] aggrTuple = state.theResults.get(state.theGBTuple);

            if (aggrTuple == null) {

                int numAggrColumns = theColumnNames.length - theNumGBColumns;
                GroupTuple gbTuple = new GroupTuple(theNumGBColumns);
                aggrTuple = new AggrValue[numAggrColumns];
                long aggrTupleSize = 0;

                for (i = 0; i < numAggrColumns; ++i) {
                    aggrTuple[i] = new AggrValue(theAggrFuncs[i], theIsRegrouping);
                    if (theCountMemory) {
                        aggrTupleSize += aggrTuple[i].sizeof();
                    }
                }

                for (i = 0; i < theNumGBColumns; ++i) {
                    gbTuple.theValues[i] = state.theGBTuple.theValues[i];
                }

                if (theCountMemory) {
                    long sz = (gbTuple.sizeof() + aggrTupleSize +
                               SizeOf.HASHMAP_ENTRY_OVERHEAD);
                    rcb.incMemoryConsumption(sz);
                }

                for (; i < theColumnNames.length; ++i) {
                    aggregate(rcb, state, aggrTuple, i,
                              inTuple.get(theColumnNames[i]));
                }

                state.theResults.put(gbTuple, aggrTuple);

                if (rcb.getTraceLevel() >= 3) {
                    rcb.trace("Started new group:\n" +
                              printResult(gbTuple, aggrTuple));
                }

                if (theNumGBColumns == theColumnNames.length) {

                    MapValue res = new MapValue();

                    for (i = 0; i < theNumGBColumns; ++i) {
                        res.put(theColumnNames[i], gbTuple.theValues[i]);
                    }

                    rcb.setRegVal(theResultReg, res);
                    return true;
                }

            } else {
                for (i = theNumGBColumns; i < theColumnNames.length; ++i) {
                    aggregate(rcb, state, aggrTuple, i,
                              inTuple.get(theColumnNames[i]));
                }

                if (rcb.getTraceLevel() >= 3) {
                    rcb.trace("Updated existing group:\n" +
                              printResult(state.theGBTuple, aggrTuple));
                }
            }
        }
    }

    private void aggregate(
        RuntimeControlBlock rcb,
        GroupIterState state,
        AggrValue[] aggrValues,
        int column,
        FieldValue val) {

        AggrValue aggrValue = aggrValues[column - theNumGBColumns];
        FuncCode aggrKind = theAggrFuncs[column - theNumGBColumns];

        switch (aggrKind) {
        case FN_COUNT:
            if (val.isNull()) {
                return;
            }

            aggrValue.add(rcb, state, theCountMemory, one,
                          rcb.getMathContext());
            return;

        case FN_COUNT_NUMBERS:
            if (val.isNull() || !val.isNumeric()) {
                return;
            }

            aggrValue.add(rcb, state, theCountMemory, one,
                          rcb.getMathContext());
            return;

        case FN_COUNT_STAR:
            aggrValue.add(rcb, state, theCountMemory, one,
                          rcb.getMathContext());
            return;

        case FN_SUM:
            if (val.isNull()) {
                return;
            }

            if (val.isNumeric()) {
               aggrValue.add(rcb, state, theCountMemory, val,
                             rcb.getMathContext());
            }
            return;

        case FN_MIN:
        case FN_MAX:
            switch (val.getType()) {
            case BINARY:
            case MAP:
            case ARRAY:
            case EMPTY:
            case NULL:
            case JSON_NULL:
                return;
            default:
                break;
            }

            FieldValue minmaxValue = (FieldValue)aggrValue.theValue;

            if (minmaxValue.isNull()) {

                if (rcb.getTraceLevel() >= 3) {
                    rcb.trace("Setting min/max to " + val);
                }

                if (theCountMemory) {
                    rcb.incMemoryConsumption(val.sizeof() - minmaxValue.sizeof());
                }
                aggrValue.theValue = val;
                return;
            }

            int cmp = Compare.compareAtomicsTotalOrder(rcb, minmaxValue, val);

            if (rcb.getTraceLevel() >= 3) {
                rcb.trace("Compared values: \n" + minmaxValue + "\n" +
                          val + "\ncomp res = " + cmp);
            }

            if (aggrKind == FuncCode.FN_MIN) {
                if (cmp <= 0) {
                    return;
                }
            } else {
                if (cmp >= 0) {
                    return;
                }
            }

            if (rcb.getTraceLevel() >= 3) {
                rcb.trace("Setting min/max to " + val);
            }

            if (theCountMemory &&
                val.getType() != minmaxValue.getType()) {
                rcb.incMemoryConsumption(val.sizeof() - minmaxValue.sizeof());
            }

            aggrValue.theValue = val;
            return;
        case FN_ARRAY_COLLECT:
        case FN_ARRAY_COLLECT_DISTINCT:
            aggrValue.collect(rcb, val, theCountMemory);
            return;
        default:
            throw new QueryStateException(
                "Method not implemented for iterator " +
                aggrKind);
        }
    }

    @SuppressWarnings("unchecked")
    private FieldValue getAggrValue(
        RuntimeControlBlock rcb,
        GroupIterState state,
        AggrValue[] aggrTuple,
        int column) {

        AggrValue aggrValue = aggrTuple[column - theNumGBColumns];
        FuncCode aggrKind = theAggrFuncs[column - theNumGBColumns];

        if (aggrKind == FuncCode.FN_SUM &&
            !aggrValue.theGotNumericInput) {
            return NullValue.getInstance();
        }

        if (aggrKind == FuncCode.FN_ARRAY_COLLECT) {
            ArrayValue collectArray = (ArrayValue)aggrValue.theValue;
            if (rcb.getRequest().inTestMode()) {
                collectArray.getArrayInternal().sort(state.theComparator);
            }
            return collectArray;
        }

        if (aggrKind == FuncCode.FN_ARRAY_COLLECT_DISTINCT) {
            ArrayValue collectArray = new ArrayValue();
            HashSet<WrappedValue> collectSet = (HashSet<WrappedValue>)
                                               aggrValue.theValue;
            Iterator<WrappedValue> iter = collectSet.iterator();
            while (iter.hasNext()) {
                collectArray.add(iter.next().theValue);
            }

            if (rcb.getRequest().inTestMode()) {
                collectArray.getArrayInternal().sort(state.theComparator);
            }
            return collectArray;
        }

        return (FieldValue)aggrValue.theValue;
    }

    private String printResult(GroupTuple gbTuple, AggrValue[] aggrValues) {

        StringBuilder sb = new StringBuilder();

        sb.append("[ ");

        for (int i = 0; i < gbTuple.theValues.length; ++i) {
            sb.append(gbTuple.theValues[i]);
            sb.append(" ");
        }

        sb.append("- ");
        for (int i = 0; i < aggrValues.length; ++i) {
            sb.append(aggrValues[i].theValue);
            sb.append(" ");
        }

        sb.append("]");
        return sb.toString();
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append("Grouping Columns : ");
        for (int i = 0; i < theNumGBColumns; ++i) {
            sb.append(theColumnNames[i]);
            if (i < theNumGBColumns - 1) {
                sb.append(", ");
            }
        }

        sb.append("\n");

        formatter.indent(sb);
        sb.append("Aggregate Functions : ");
        for (int i = 0; i < theAggrFuncs.length; ++i) {
            sb.append(theAggrFuncs[i]);
            if (i < theAggrFuncs.length - 1) {
                sb.append(",\n");
            }
        }
        sb.append("\n");
        theInput.display(sb, formatter);
    }
}
