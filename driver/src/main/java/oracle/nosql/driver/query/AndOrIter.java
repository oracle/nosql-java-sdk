/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;

import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.BooleanValue;
import oracle.nosql.driver.values.NullValue;

public class AndOrIter extends PlanIter {

    private final FuncCode theCode;

    private final PlanIter[] theArgs;

    public AndOrIter(ByteInputStream in, short queryVersion)
        throws IOException {

        super(in, queryVersion);
        short ordinal = in.readShort();
        theCode = FuncCode.valueOf(ordinal);
        theArgs = deserializeIters(in, queryVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.AND_OR;
    }

    @Override
    FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        for (PlanIter arg : theArgs) {
            arg.open(rcb);
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        for (PlanIter arg : theArgs) {
            arg.reset(rcb);
        }
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        for (PlanIter arg : theArgs) {
            arg.close(rcb);
        }

        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        /*
         * If AND, start true, and exit as soon as there is a false result.
         * If OR, start false, and exit as soon as there is a true result.
         */
        assert(theCode == FuncCode.OP_AND || theCode == FuncCode.OP_OR);
        boolean result = (theCode == FuncCode.OP_AND ? true : false);
        boolean haveNull = false;
        FieldValue res;

        for (PlanIter arg : theArgs) {

            boolean more = arg.next(rcb);

            boolean argResult;

            if (!more) {
                argResult = false;
            } else {
                FieldValue argVal = rcb.getRegVal(arg.getResultReg());

                if (argVal.isNull()) {
                    haveNull = true;
                    continue;
                }

                argResult = argVal.getBoolean();
            }

            if (theCode == FuncCode.OP_AND) {
                result &= argResult;
                if (!result) {
                    haveNull = false;
                    break;
                }
            } else {
                result |= argResult;
                if (result) {
                    haveNull = false;
                    break;
                }
            }
        }

        if (haveNull) {
            res = NullValue.getInstance();
        } else {
            res = BooleanValue.getInstance(result);
        }

        rcb.setRegVal(theResultReg, res);
        state.done();
        return true;
    }


    @Override
    protected void displayContent(
        StringBuilder sb,
        QueryFormatter formatter) {

        displayInputIters(sb, formatter, theArgs);
    }

    @Override
    void displayName(StringBuilder sb) {
        if (theCode == FuncCode.OP_AND) {
            sb.append("AND");
        } else {
            sb.append("OR");
        }
    }
}
