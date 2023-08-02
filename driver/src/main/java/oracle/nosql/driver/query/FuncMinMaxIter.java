/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;

import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.util.ByteInputStream;

/*
 * any_atomic min(any*)
 * any_atomic max(any*)
 *
 * Implements the MIN/MAX aggregate functions. It is needed by the driver
 * to compute the total min/max from the partial mins/maxs received from the
 * proxy.
 */
public class FuncMinMaxIter extends PlanIter {

    private final FuncCode theFuncCode;

    private final PlanIter theInput;

    FuncMinMaxIter(ByteInputStream in, short serialVersion) throws IOException {
        super(in, serialVersion);
        short ordinal = in.readShort();
        theFuncCode = FuncCode.valueOf(ordinal);
        theInput = deserializeIter(in, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FN_MIN_MAX;
    }

    @Override
    FuncCode getFuncCode() {
        return theFuncCode;
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

            minmaxNewVal(rcb, state, theFuncCode, val);
        }
    }

    private static void minmaxNewVal(
        RuntimeControlBlock rcb,
        AggrIterState state,
        FuncCode fncode,
        FieldValue val) {


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

        if (state.theMinMax.isNull()) {
            state.theMinMax = val;
            return;
        }

        int cmp = Compare.compareAtomicsTotalOrder(rcb, state.theMinMax, val);

        if (rcb.getTraceLevel() >= 3) {
            rcb.trace("Compared values: \n" + state.theMinMax + "\n" + val +
                      "\ncomp res = " + cmp);
        }

        if (fncode == FuncCode.FN_MIN) {
            if (cmp <= 0) {
                return;
            }
        } else {
            if (cmp >= 0) {
                return;
            }
        }

        if (rcb.getTraceLevel() >= 2) {
            rcb.trace("Setting min/max to " + val);
        }

        state.theMinMax = val;
    }

    @Override
    FieldValue getAggrValue(RuntimeControlBlock rcb, boolean reset) {

        AggrIterState state = (AggrIterState)rcb.getState(theStatePos);
        FieldValue res = state.theMinMax;

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
