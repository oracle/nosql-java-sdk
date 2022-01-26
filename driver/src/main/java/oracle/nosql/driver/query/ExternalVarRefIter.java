/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;

import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;

/**
 * In general, ExternalVarRefIter represents a reference to an external variable
 * in the query. Such a reference will need to be "executed" at the driver side
 * when the variable appears in the OFFSET or LIMIT clause.
 *
 * ExternalVarRefIter simply returns the value that the variable is currently
 * bound to. This value is set by the app via the methods of QueryRequest.
 *
 * theName:
 * The name of the variable. Used only when displaying the execution plan
 * and in error messages.
 *
 * theId:
 * The variable id. It is used as an index into an array of FieldValues
 * in the RCB that stores the values of the external vars.
 */
public class ExternalVarRefIter extends PlanIter {

    private final String theName;

    private final int theId;

    ExternalVarRefIter(
        ByteInputStream in,
        short serialVersion) throws IOException {

        super(in, serialVersion);
        theName = SerializationUtil.readString(in);
        theId = readPositiveInt(in);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.EXTERNAL_VAR_REF;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        FieldValue val = rcb.getExternalVar(theId);

        /*
         * val should not be null, because we check before starting query
         * execution that all the external vars have been bound. So this is
         * a sanity check.
         */
        if (val == null) {
            throw new QueryStateException(
                "Variable " + theName + " has not been set");
        }

        rcb.setRegVal(theResultReg, val);
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
        sb.append("EXTENAL_VAR_REF(");
        sb.append(theName);
        sb.append(", ").append(theId);
        sb.append(")");
    }
}
