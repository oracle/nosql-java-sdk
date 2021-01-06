/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;

import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;

/**
 * VarRefIter represents a reference to a non-external variable in the query.
 * It simply returns the value that the variable is currently bound to. This
 * value is computed by the variable's "domain iterator" (the iterator that
 * evaluates the domain expression of the variable). The domain iterator stores
 * the value in theResultReg of this VarRefIter.
 *
 * In the context of the driver, an implicit internal variable is used
 * to represent the results arriving from the proxy. All other expressions that
 * are computed at the driver operate on these results, so all such expressions
 * reference this variable. This is analogous to the internal variable used in
 * kvstore to represent the table alias in the FROM clause.
 *
 * theName:
 * The name of the variable. Used only when displaying the execution plan.
 */
public class VarRefIter extends PlanIter {

    private final String theName;

    VarRefIter(ByteInputStream in, short serialVersion) throws IOException {

        super(in, serialVersion);
        theName = SerializationUtil.readString(in);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.VAR_REF;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            if (rcb.getTraceLevel() >= 4) {
                rcb.trace("No Value for variable " + theName + " in register " +
                          theResultReg);
            }
            return false;
        }

        if (rcb.getTraceLevel() >= 4) {
            rcb.trace("Value for variable " + theName + " in register " +
                      theResultReg + ":\n" + rcb.getRegVal(theResultReg));
        }

        state.done();
        return true;
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        state.close();
    }

    @Override
    protected void display(StringBuilder sb, QueryFormatter formatter) {
        formatter.indent(sb);
        displayContent(sb, formatter);
        displayRegs(sb);
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        sb.append("VAR_REF(");
        sb.append(theName);
        sb.append(")");
    }
}
