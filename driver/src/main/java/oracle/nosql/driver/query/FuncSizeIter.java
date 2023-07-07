/*-
 * Copyright (c) 2011, 2023 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;

import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.util.ByteInputStream;

public class FuncSizeIter extends PlanIter {

    private final PlanIter theInput;

    FuncSizeIter(ByteInputStream in, short serialVersion) throws IOException {
        super(in, serialVersion);
        theInput = deserializeIter(in, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FN_SIZE;
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

        PlanIterState state = rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean more = theInput.next(rcb);

        if (!more) {
            state.done();
            return false;
        }

        int size;

        FieldValue item = rcb.getRegVal(theInput.getResultReg());

        if (item.isNull()) {
            rcb.setRegVal(theResultReg, NullValue.getInstance());
            state.done();
            return true;
        }

        if (item.isArray()) {
            size = ((ArrayValue)item).size();
        } else if (item.isMap()) {
            size = ((MapValue)item).size();
        } else {
            throw new QueryException(
                "Input to the size() function has wrong type\n" +
                "Expected a complex item. Actual item type is:\n" +
                item.getType(), getLocation());
        }

        FieldValue res = new IntegerValue(size);
        rcb.setRegVal(theResultReg, res);
        return true;
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {
        theInput.display(sb, formatter);
    }
}
