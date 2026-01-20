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
import oracle.nosql.driver.values.ArrayValue;

public class ArrayConstrIter extends PlanIter {

    private final PlanIter[] theArgs;

    private final boolean theIsConditional;

    public ArrayConstrIter(ByteInputStream in, short queryVersion)
        throws IOException {

        super(in, queryVersion);
        theIsConditional = in.readBoolean();
        theArgs = deserializeIters(in, queryVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.ARRAY_CONSTRUCTOR;
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

        ArrayValue array;

        if (theIsConditional) {

            boolean more = theArgs[0].next(rcb);

            if (!more) {
                state.done();
                return false;
            }

            FieldValue val = rcb.getRegVal(theArgs[0].getResultReg());

            more = theArgs[0].next(rcb);

            if (!more) {
                rcb.setRegVal(theResultReg, val);
                state.done();
                return true;
            }

            array = new ArrayValue();

            if (!val.isNull()) {
                array.add(val);
            }

            val = rcb.getRegVal(theArgs[0].getResultReg());

            if (!val.isNull()) {
                array.add(val);
            }

        } else {
            array = new ArrayValue();
        }

        for (int currArg = 0; currArg < theArgs.length; ++currArg) {

            while (true) {
                boolean more = theArgs[currArg].next(rcb);

                if (!more) {
                    break;
                }

                FieldValue val =
                    rcb.getRegVal(theArgs[currArg].getResultReg());

                if (val.isNull()) {
                    continue;
                }

                array.add(val);
            }
        }

        rcb.setRegVal(theResultReg, array);
        state.done();
        return true;
    }


    @Override
    protected void displayContent(
        StringBuilder sb,
        QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append("\"conditional\" : ").append(theIsConditional);
        sb.append(",\n");
        displayInputIters(sb, formatter, theArgs);
    }
}
