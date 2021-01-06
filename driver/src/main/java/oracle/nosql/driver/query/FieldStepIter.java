/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;

import oracle.nosql.driver.values.EmptyValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.FieldValue.Type;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;

/**
 * FieldStepIter returns the value of a field in an input MapValue. It is
 * used by the driver to implement column references in the SELECT
 * list (see SFWIter).
 */
public class FieldStepIter extends PlanIter {

    private final PlanIter theInputIter;

    private final String theFieldName;

    FieldStepIter(ByteInputStream in, short serialVersion) throws IOException {

        super(in, serialVersion);
        theInputIter = deserializeIter(in, serialVersion);
        theFieldName = SerializationUtil.readString(in);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.FIELD_STEP;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new PlanIterState());
        theInputIter.open(rcb);
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state.isDone()) {
            return false;
        }

        int inputReg = theInputIter.getResultReg();

        while (true) {

            FieldValue ctxItem = null;
            Type ctxItemKind;
            FieldValue result;

            boolean more = theInputIter.next(rcb);

            ctxItem = rcb.getRegVal(inputReg);

            if (!more || ctxItem == EmptyValue.getInstance()) {
                state.done();
                return false;
            }

            ctxItem = rcb.getRegVal(inputReg);

            if (ctxItem.isAtomic()) {
                continue;
            }

            if (ctxItem == NullValue.getInstance()) {
                rcb.setRegVal(theResultReg, ctxItem);
                return true;
            }

            ctxItemKind = ctxItem.getType();

            if (ctxItemKind != Type.MAP) {
                throw new QueryStateException(
                    "Input value in field step has invalid type.\n" +
                    ctxItem);
            }

            MapValue map = (MapValue)ctxItem;
            result = map.get(theFieldName);

            if (result == null) {
                continue;
            }

            rcb.setRegVal(theResultReg, result);
            return true;
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theInputIter.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theInputIter.close(rcb);
        state.close();
    }

   @Override
   protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

       theInputIter.display(sb, formatter);

       sb.append(",\n");
       formatter.indent(sb);
       sb.append(theFieldName);
   }
}
