/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;

import oracle.nosql.driver.query.QueryException.Location;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.BooleanValue;

public class IsNullIter extends PlanIter {

    private final FuncCode theCode;

    private final PlanIter theArg;

    public IsNullIter(ByteInputStream in, short queryVersion)
        throws IOException {

        super(in, queryVersion);
        short ordinal = in.readShort();
        theCode = FuncCode.valueOf(ordinal);
        theArg = deserializeIter(in, queryVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.IS_NULL;
    }

    @Override
    FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        theArg.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theArg.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theArg.close(rcb);

        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean more = theArg.next(rcb);

        if (!more) {
            if (theCode == FuncCode.OP_IS_NULL) {
                rcb.setRegVal(theResultReg, BooleanValue.getInstance(false));
            } else {
                rcb.setRegVal(theResultReg, BooleanValue.getInstance(true));
            }

            state.done();
            return true;
        }

        FieldValue val = rcb.getRegVal(theArg.getResultReg());

        if (theCode == FuncCode.OP_IS_NULL) {
            if (val.isNull()) {
                rcb.setRegVal(theResultReg, BooleanValue.getInstance(true));
            } else {
                rcb.setRegVal(theResultReg, BooleanValue.getInstance(false));
            }
        } else {
            if (val.isNull()) {
                rcb.setRegVal(theResultReg, BooleanValue.getInstance(false));
            } else {
                rcb.setRegVal(theResultReg, BooleanValue.getInstance(true));
            }
        }

        state.done();
        return true;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        QueryFormatter formatter) {
        displayInputIter(sb, formatter, theArg);
    }
}
