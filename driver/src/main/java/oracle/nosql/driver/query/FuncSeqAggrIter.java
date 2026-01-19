/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;
import java.math.BigDecimal;

import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.NumberValue;

public class FuncSeqAggrIter extends PlanIter {

    private final FuncCode theCode;

    private final PlanIter theInput;

    public FuncSeqAggrIter(ByteInputStream in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        short ordinal = in.readShort();
        theCode = FuncCode.valueOf(ordinal);
        theInput = deserializeIter(in, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.SEQ_AGGR;
    }

    @Override
    FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new AggrIterState());
        theInput.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theInput.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
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

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean more = theInput.next(rcb);

        if (!more) {
            state.done();

            if (theCode == FuncCode.FN_SEQ_COUNT ||
                theCode == FuncCode.FN_SEQ_COUNT_I) {
                rcb.setRegVal(theResultReg, new LongValue(0));
                              
                return true;
            }

            return false;
        }

        switch (theCode) {
        case FN_SEQ_COUNT:
        case FN_SEQ_COUNT_I:
            nextCount(rcb, state);
            break;
        case FN_SEQ_COUNT_NUMBERS_I:
            nextCountNumbers(rcb, state);
            break;
        case FN_SEQ_SUM:
        case FN_SEQ_AVG:
            nextSumAvg(rcb, state);
            break;
        case FN_SEQ_MIN:
        case FN_SEQ_MAX:
        case FN_SEQ_MIN_I:
        case FN_SEQ_MAX_I:
            nextMinMax(rcb, state);
            break;
        default:
            throw new QueryStateException("Unexpected function: " + theCode);
        }

        state.done();
        return true;
    }

    private void nextCount(RuntimeControlBlock rcb, AggrIterState state) {

        boolean more = true;

        while (more) {

            FieldValue val = rcb.getRegVal(theInput.getResultReg());

            if (val.isNull()) {
                if (theCode == FuncCode.FN_SEQ_COUNT) {
                    rcb.setRegVal(theResultReg, NullValue.getInstance());
                    return;
                }
                more = theInput.next(rcb);
                continue;
            }

            ++state.theCount;
            more = theInput.next(rcb);
        }

        rcb.setRegVal(theResultReg, new LongValue(state.theCount));
    }

    private void nextCountNumbers(RuntimeControlBlock rcb, AggrIterState state) {

        boolean more = true;

        while (more) {

            FieldValue val = rcb.getRegVal(theInput.getResultReg());

            if (val.isNumeric()) {
                ++state.theCount;
            }

            more = theInput.next(rcb);
        }

        rcb.setRegVal(theResultReg, new LongValue(state.theCount));
    }

    private void nextSumAvg(RuntimeControlBlock rcb, AggrIterState state) {

        boolean more = true;

        while (more) {

            FieldValue val = rcb.getRegVal(theInput.getResultReg());

            FuncSumIter.sumNewValue(state, val);

            more = theInput.next(rcb);
        }

        if (!state.theGotNumericInput) {
             rcb.setRegVal(theResultReg, NullValue.getInstance());
             return;
        }

        FieldValue res = null;

        if (theCode == FuncCode.FN_SEQ_SUM) {

            switch (state.theSumType) {
            case LONG:
                res = new LongValue(state.theLongSum);
                break;
            case DOUBLE:
                res = new DoubleValue(state.theDoubleSum);
                break;
            case NUMBER:
                res = new NumberValue(state.theNumberSum);
                break;
            default:
                throw new QueryStateException(
                    "Unexpected result type for SUM function: " +
                    state.theSumType);
            }
        } else {
            double avg;

            switch (state.theSumType) {
            case LONG:
                avg = state.theLongSum / (double)state.theCount;
                res = new DoubleValue(avg);
                break;
            case DOUBLE:
                avg = state.theDoubleSum / state.theCount;
                res = new DoubleValue(avg);
                break;
            case NUMBER:
                BigDecimal bcount = new BigDecimal(state.theCount);
                BigDecimal bavg = state.theNumberSum.
                    divide(bcount, rcb.getMathContext());
                res = new NumberValue(bavg);
                break;
            default:
                throw new QueryStateException(
                    "Unexpected result type for SUM function: " +
                    state.theSumType);
            }
        }

        rcb.setRegVal(theResultReg, res);
    }

    private void nextMinMax(RuntimeControlBlock rcb, AggrIterState state) {

        boolean more = true;

        while (more) {

            FieldValue val = rcb.getRegVal(theInput.getResultReg());

            if (val.isNull() &&
                (theCode == FuncCode.FN_SEQ_MIN ||
                 theCode == FuncCode.FN_SEQ_MAX)) {
                rcb.setRegVal(theResultReg, val);
                return;
            }

            FuncMinMaxIter.minmaxNewVal(rcb, state, theCode, val);

            more = theInput.next(rcb);
        }

        rcb.setRegVal(theResultReg, state.theMinMax);
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        QueryFormatter formatter) {
        displayInputIter(sb, formatter, theInput);
    }
}
