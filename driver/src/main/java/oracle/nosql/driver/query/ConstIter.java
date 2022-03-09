/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;

import oracle.nosql.driver.ops.serde.BinaryProtocol;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.util.ByteInputStream;

/**
 * ConstIter represents a reference to a constant value in the query.
 * Such a reference will need to be "executed" at the driver side when
 * the constant appears in the OFFSET or LIMIT clause.
 */
public class ConstIter extends PlanIter {

    final FieldValue theValue;

    ConstIter(ByteInputStream in, short serialVersion) throws IOException {

        super(in, serialVersion);
        theValue = BinaryProtocol.readFieldValue(in);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.CONST;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        rcb.setRegVal(theResultReg, theValue);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
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

    FieldValue getValue() {
        return theValue;
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        formatter.indent(sb);
        sb.append(theValue.toJson());
    }
}
