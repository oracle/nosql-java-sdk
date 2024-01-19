/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;
import java.math.BigDecimal;

import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.FieldValue.Type;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.util.ByteInputStream;

/**
 *  any_atomic sum(any*)
 *
 * Implements the SUM aggregate function. It is needed by the driver to
 * re-sum partial sums and counts received from the proxy.
 *
 * Note: The next() method does not actually return a value; it just adds a new
 * value (if it is of a numeric type) to the running sum kept in the state. Also
 * the reset() method resets the input iter (so that the next input value can be
 * computed), but does not reset the FuncSumState. The state is reset, and the
 * current sum value is returned, by the getAggrValue() method.
 */
public class FuncSumIter extends PlanIter {

    private final PlanIter theInput;

    FuncSumIter(ByteInputStream in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theInput = deserializeIter(in, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FN_SUM;
    }

    @Override
    PlanIter getInputIter() {
        return theInput;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new AggrIterState());
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

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);

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
                rcb.trace("Summing up value " + val);
            }

            if (val.isNull()) {
                continue;
            }

            state.theNullInputOnly = false;

            sumNewValue(state, val);
        }
    }

    static void sumNewValue(AggrIterState state, FieldValue val) {

        BigDecimal bd;

        switch (val.getType()) {
        case INTEGER: {
            ++state.theCount;
            switch (state.theSumType) {
            case LONG:
                state.theLongSum += ((IntegerValue)val).getValue();
                break;
            case DOUBLE:
                state.theDoubleSum += ((IntegerValue)val).getValue();
                break;
            case NUMBER:
                bd = new BigDecimal(((IntegerValue)val).getValue());
                state.theNumberSum = state.theNumberSum.add(bd);
                break;
            default:
                assert(false);
            }
            break;
        }
        case LONG: {
            ++state.theCount;
            switch (state.theSumType) {
            case LONG:
                state.theLongSum += ((LongValue)val).getValue();
                break;
            case DOUBLE:
                state.theDoubleSum += ((LongValue)val).getValue();
                break;
            case NUMBER:
                bd = new BigDecimal(((LongValue)val).getValue());
                state.theNumberSum = state.theNumberSum.add(bd);
                break;
            default:
                assert(false);
            }
            break;
        }
        case DOUBLE: {
            ++state.theCount;
            switch (state.theSumType) {
            case LONG:
                state.theDoubleSum = state.theLongSum;
                state.theDoubleSum += ((DoubleValue)val).getValue();
                state.theSumType = Type.DOUBLE;
                break;
            case DOUBLE:
                state.theDoubleSum += ((DoubleValue)val).getValue();
                break;
            case NUMBER:
                bd = new BigDecimal(((DoubleValue)val).getValue());
                state.theNumberSum = state.theNumberSum.add(bd);
                break;
            default:
                assert(false);
            }
            break;
        }
        case NUMBER: {
            ++state.theCount;
            if (state.theNumberSum == null) {
                state.theNumberSum = new BigDecimal(0);
            }

            switch (state.theSumType) {
            case LONG:
                state.theNumberSum =  new BigDecimal(state.theLongSum);
                state.theNumberSum =
                    state.theNumberSum.add(((NumberValue)val).getValue());
                state.theSumType = Type.NUMBER;
                break;
            case DOUBLE:
                state.theNumberSum =  new BigDecimal(state.theDoubleSum);
                state.theNumberSum =
                    state.theNumberSum.add(((NumberValue)val).getValue());
                state.theSumType = Type.NUMBER;
                break;
            case NUMBER:
                state.theNumberSum =
                    state.theNumberSum.add(((NumberValue)val).getValue());
                break;
            default:
                assert(false);
            }
            break;
        }
        default:
            break;
        }
    }

    /*
     * This method is called twice when a group completes and a new group
     * starts. In both cases it returns the current value of the SUM that is
     * stored in the FuncSumState. The 1st time, the SUM value is the final
     * SUM value for the just completed group. In this case the "reset" param
     * is true in order to reset the running sum in the state. The 2nd time
     * the SUM value is the inital SUM value computed from the 1st tuple of
     * the new group.
     */
    @Override
    FieldValue getAggrValue(RuntimeControlBlock rcb, boolean reset) {

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);
        FieldValue res = null;

        if (state.theNullInputOnly) {
            return NullValue.getInstance();
        }

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
                "Unexpected result type for SUM function: " + state.theSumType);
        }

        if (rcb.getTraceLevel() >= 4) {
            rcb.trace("Computed sum = " + res);
        }

        if (reset) {
            state.reset(this);
        }
        return res;
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        theInput.display(sb, formatter);
    }
}
