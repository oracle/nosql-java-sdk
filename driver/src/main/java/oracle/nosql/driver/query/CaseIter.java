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

public class CaseIter extends PlanIter {

    static private class CaseIterState extends PlanIterState {

        /* theActiveIter is set to the iterator whose associated condition
         * evaluated to true. */
        PlanIter theActiveIter;
    }

    private final PlanIter[] theCondIters;

    private final PlanIter[] theThenIters;

    private final PlanIter theElseIter;

    public CaseIter(ByteInputStream in, short queryVersion)
        throws IOException {

        super(in, queryVersion);
        theCondIters = deserializeIters(in, queryVersion);
        theThenIters = deserializeIters(in, queryVersion);
        theElseIter = deserializeIter(in, queryVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.CASE;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new CaseIterState());

        for (PlanIter iter : theCondIters) {
            iter.open(rcb);
        }
        for (PlanIter iter : theThenIters) {
            iter.open(rcb);
        }
        if (theElseIter != null) {
            theElseIter.open(rcb);
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        for (PlanIter iter : theCondIters) {
            iter.reset(rcb);
        }
        for (PlanIter iter : theThenIters) {
            iter.reset(rcb);
        }
        if (theElseIter != null) {
            theElseIter.reset(rcb);
        }

        CaseIterState state = (CaseIterState)rcb.getState(theStatePos);
        state.reset(this);
    }
    @Override
    public void close(RuntimeControlBlock rcb) {

        CaseIterState state = (CaseIterState)rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        for (PlanIter iter : theCondIters) {
            iter.close(rcb);
        }
        for (PlanIter iter : theThenIters) {
            iter.close(rcb);
        }
        if (theElseIter != null) {
            theElseIter.close(rcb);
        }
        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        CaseIterState state = (CaseIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        if (state.isOpen()) {

            int i;
            for (i = 0; i < theCondIters.length; ++i) {

                boolean more = theCondIters[i].next(rcb);

                if (!more) {
                    continue;
                }
 
                FieldValue val =
                    rcb.getRegVal(theCondIters[i].getResultReg());

                if (val.isNull() || !val.getBoolean()) {
                    continue;
                }

                state.theActiveIter = theThenIters[i];
                break;
            }

            if (i == theCondIters.length) {
                if (theElseIter == null) {
                    state.done();
                    return false;
                }
                state.theActiveIter = theElseIter;
            }

            state.setState(PlanIterState.StateEnum.RUNNING);
        }

        if (!state.theActiveIter.next(rcb)) {
            state.done();
            return false;
        }

        FieldValue retValue = rcb.getRegVal(state.theActiveIter.getResultReg());
        rcb.setRegVal(theResultReg, retValue);
        return true;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        QueryFormatter formatter) {

        formatter.indent(sb);
        sb.append("\"clauses\" : [\n");

        formatter.incIndent();

        for (int i = 0; i < theCondIters.length; ++i) {

            formatter.indent(sb);
            sb.append("{\n");
            formatter.incIndent();
            formatter.indent(sb);
            sb.append("\"when iterator\" :\n");
            theCondIters[i].display(sb, formatter);
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("\"then iterator\" :\n");
            theThenIters[i].display(sb, formatter);
            sb.append("\n");
            formatter.decIndent();
            formatter.indent(sb);
            sb.append("}");

            if (i < theCondIters.length - 1) {
                sb.append(",\n");
            }
        }

        if (theElseIter != null) {
            sb.append(",\n");
            formatter.indent(sb);
            sb.append("{\n");
            formatter.incIndent();
            formatter.indent(sb);
            sb.append("\"else iterator\" :\n");
            theElseIter.display(sb, formatter);
            sb.append("\n");
            formatter.decIndent();
            formatter.indent(sb);
            sb.append("}");
        }

        formatter.decIndent();
        sb.append("\n");
        formatter.indent(sb);
        sb.append("]");
    }
}
