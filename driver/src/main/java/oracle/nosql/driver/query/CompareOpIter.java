/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;
import java.util.Map;

import oracle.nosql.driver.query.QueryException.Location;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;
import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.FieldValue.Type;
import oracle.nosql.driver.values.BooleanValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NullValue;

public class CompareOpIter extends PlanIter {

    static public class CompResult {

        public int comp;
        public boolean incompatible;
        public boolean haveNull;

        void clear() {
            comp = 0;
            incompatible = false;
            haveNull = false;
        }

        @Override
        public String toString() {
            return ("(comp, incompatible, haveNull) = (" + 
                    comp + ", " + incompatible + ", " + haveNull + ")");
        }
    }

    static private class CompIterState extends PlanIterState {

        final CompResult theResult = new CompResult();

        @Override
        public void reset(PlanIter iter) {
            super.reset(iter);
            theResult.clear();
        }
    }

    private final FuncCode theCode;

    private final PlanIter theLeftOp;

    private final PlanIter theRightOp;

    public CompareOpIter(ByteInputStream in, short serialVersion)
        throws IOException {

        super(in, serialVersion);
        short ordinal = in.readShort();
        theCode = FuncCode.valueOf(ordinal);
        theLeftOp = deserializeIter(in, serialVersion);
        theRightOp = deserializeIter(in, serialVersion);
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.VALUE_COMPARE;
    }

    @Override
    FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {
        rcb.setState(theStatePos, new CompIterState());
        theLeftOp.open(rcb);
        theRightOp.open(rcb);
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {
        theLeftOp.reset(rcb);
        theRightOp.reset(rcb);
        PlanIterState state = rcb.getState(theStatePos);
        state.reset(this);
    }

    @Override
    public void close(RuntimeControlBlock rcb) {

        PlanIterState state = rcb.getState(theStatePos);
        if (state == null) {
            return;
        }

        theLeftOp.close(rcb);
        theRightOp.close(rcb);
        state.close();
    }

    @Override
    public boolean next(RuntimeControlBlock rcb) {

        CompIterState state = (CompIterState)rcb.getState(theStatePos);

        if (state.isDone()) {
            return false;
        }

        boolean leftOpNext = theLeftOp.next(rcb);

        if (leftOpNext && theLeftOp.next(rcb)) {
            throw new QueryException(
                "The left operand of comparison operator " + theCode +
                " is a sequence with more than one items. Comparison " +
                "operators cannot operate on sequences of more than one items.",
                theLocation);
        }

        boolean rightOpNext = theRightOp.next(rcb);

        if (rightOpNext && theRightOp.next(rcb)) {
            throw new QueryException(
                "The right operand of comparison operator " + theCode +
                " is a sequence with more than one items. Comparison " +
                "operators cannot operate on sequences of more than one items.",
                theLocation);
        }

        if (!rightOpNext && !leftOpNext) {
            /* both sides are empty */
            state.theResult.comp = 0;

        } else if (!rightOpNext || !leftOpNext) {
            /* only one of the sides is empty */
            if (theCode != FuncCode.OP_NEQ) {
                /* this will be converted to false */
                state.theResult.incompatible = true;
            } else {
                /* this will be converted to true */
                state.theResult.comp = 1;
            }

        } else {
            FieldValue lvalue = rcb.getRegVal(theLeftOp.getResultReg());
            FieldValue rvalue = rcb.getRegVal(theRightOp.getResultReg());

            assert(lvalue != null && rvalue != null);

            compare(rcb,
                    lvalue,
                    rvalue,
                    theCode,
                    state.theResult,
                    getLocation());
        }

        if (state.theResult.haveNull) {
            rcb.setRegVal(theResultReg, NullValue.getInstance());
            state.done();
            return true;
        }

        if (state.theResult.incompatible) {
            rcb.setRegVal(theResultReg, BooleanValue.getInstance(false));
            state.done();
            return true;
        }

        int comp = state.theResult.comp;
        boolean result;

        switch (theCode) {
        case OP_EQ:
            result = (comp == 0);
            break;
        case OP_NEQ:
            result = (comp != 0);
            break;
        case OP_GT:
            result = (comp > 0);
            break;
        case OP_GE:
            result = (comp >= 0);
            break;
        case OP_LT:
            result = (comp < 0);
            break;
        case OP_LE:
            result = (comp <= 0);
            break;
        default:
            throw new QueryStateException(
                "Invalid operation code: " + theCode);
        }

        FieldValue res = BooleanValue.getInstance(result);
        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    /*
     * Compare 2 values for the order-relation specified by the given opCode.
     * If the values are complex, the method will, in general, call itself
     * recursivelly on the contained values.
     *
     * The method retuns 3 pieces of info (inside the "res" out param):
     * 
     * a. Whether either v0 or v1 is NULL.
     * b. Whether the values are not cmparable
     * c1. If both a and b are false and the operator is = or !=, an integer which is
     *     equal to 0 if v0 == v1, and non-0 if v0 != v1.
     * c2. If both a nd b are false and the operator is >, >=, <, or <=, an integer
     *     which is equal to 0 if v0 == v1, greater than 0 if v0 > v1, and
     *     less than zero if v0 < v1.
     */
    public static void compare(
        RuntimeControlBlock rcb,
        FieldValue v0,
        FieldValue v1,
        FuncCode opCode,
        CompResult res,
        Location location) {

        if (rcb != null && rcb.getTraceLevel() >= 4) {
            rcb.trace("Comparing values: \n" + v0 + "\n" + v1);
        }

        res.clear();

        if (v0.isNull() || v1.isNull()) {
            res.haveNull = true;
            return;
        }

        if (v0.isJsonNull()) {

            if (v1.isJsonNull()) {
                res.comp = 0;
                return;
            }

            if (opCode != FuncCode.OP_NEQ) {
                /* this will be converted to false */
                res.incompatible = true;
                return;
            }

            /* this will be converted to true */
            res.comp = 1;
            return;
        }

        if (v1.isJsonNull()) {

            if (opCode != FuncCode.OP_NEQ) {
                /* this will be converted to false */
                res.incompatible = true;
                return;
            }

            /* this will be converted to true */
            res.comp = 1;
            return;
        }

        Type tc0 = v0.getType();
        Type tc1 = v1.getType();

        switch (tc0) {

        case EMPTY:
            if (tc1 == Type.EMPTY) {
                if (opCode == FuncCode.OP_EQ ||
                    opCode == FuncCode.OP_GE ||
                    opCode == FuncCode.OP_LE) {
                    res.comp = 0;
                } else {
                   res.incompatible = true;
                }

                return;
            }

            if (opCode == FuncCode.OP_NEQ) {
                res.comp = 1;
            } else {
                res.incompatible = true;
            }

            return;

        case INTEGER: {
            switch (tc1) {
            case INTEGER:
                res.comp = v0.compareTo(v1);
                return;
            case LONG:
                res.comp = -v1.compareTo(v0);
                return;
            case DOUBLE:
                res.comp = Double.compare(v0.getInt(), v1.getDouble());
                return;
            case NUMBER:
                res.comp = -v1.compareTo(v0);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case LONG: {
            switch (tc1) {
            case INTEGER:
                res.comp = v0.compareTo(v1);
                return;
            case LONG:
                res.comp = v0.compareTo(v1);
                return;
            case DOUBLE:
                res.comp = Double.compare(v0.getLong(), v1.getDouble());
                return;
            case NUMBER:
                res.comp = -v1.compareTo(v0);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case DOUBLE: {
            switch (tc1) {
            case INTEGER:
                res.comp = Double.compare(v0.getDouble(), v1.getInt());
                return;
            case LONG:
                res.comp = Double.compare(v0.getDouble(), v1.getLong());
                return;
            case DOUBLE:
                res.comp = Double.compare(v0.getDouble(), v1.getDouble());
                return;
            case NUMBER:
                res.comp = -v1.compareTo(v0);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case NUMBER: {
            switch (tc1) {
            case NUMBER:
            case DOUBLE:
            case INTEGER:
            case LONG:
                res.comp = v0.compareTo(v1);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case STRING: {
            switch (tc1) {
            case STRING:
            case TIMESTAMP:
                res.comp = v0.getString().compareTo(v1.getString());

                if (rcb != null && rcb.getTraceLevel() >= 3) {
                    rcb.trace("Comparing STRING " + v0 + " with " + v1);
                    rcb.trace("res.comp = " + res.comp);
                }

                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case BOOLEAN: {
            switch (tc1) {
            case BOOLEAN:
                res.comp = v0.compareTo(v1);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case BINARY: {
            switch (tc1) {
            case BINARY:
                if (opCode != FuncCode.OP_EQ && opCode != FuncCode.OP_NEQ) {
                    res.incompatible = true;
                    return;
                }
                res.comp = v0.compareTo(v1);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case TIMESTAMP: {
            switch (tc1) {
            case TIMESTAMP:
            case STRING:
                res.comp = v0.getString().compareTo(v1.getString());

                if (rcb != null && rcb.getTraceLevel() >= 3) {
                    rcb.trace("Comparing TIMESTAMP " + v0 + " with " + v1);
                    rcb.trace("res.comp = " + res.comp);
                }

                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case MAP: {
            switch (tc1) {
            case MAP:
                if (opCode != FuncCode.OP_EQ && opCode != FuncCode.OP_NEQ) {
                    res.incompatible = true;
                    return;
                }

                MapValue m0 = (MapValue)v0;
                MapValue m1 = (MapValue)v1;
                compareMaps(rcb, m0, m1, opCode, res, location);
                return;
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        case ARRAY: {
            switch (tc1) {
            case ARRAY:
                ArrayValue a0 = (ArrayValue)v0;
                ArrayValue a1 = (ArrayValue)v1;

                if (opCode == FuncCode.OP_EQ || opCode == FuncCode.OP_NEQ) {
                    if (a0.size() != a1.size()) {
                        res.comp = 1;
                        return;
                    }
                }

                int minSize = Math.min(a0.size(), a1.size());

                for (int i = 0; i < minSize; ++i) {

                    FieldValue elem0 = a0.get(i);
                    FieldValue elem1 = a1.get(i);
                    assert(elem0 != null);
                    assert(elem1 != null);

                    compare(rcb, elem0, elem1, opCode, res, location);

                    if (res.comp != 0 || res.haveNull || res.incompatible) {
                        return;
                    }
                }

                if (a0.size() != minSize) {
                    res.comp = 1;
                    return;
                } else if (a1.size() != minSize) {
                    res.comp = -1;
                    return;
                } else {
                    res.comp = 0;
                    return;
                }
            case EMPTY:
                if (opCode == FuncCode.OP_NEQ) {
                    res.comp = 1;
                } else {
                    res.incompatible = true;
                }
                return;
            default:
                res.incompatible = true;
                return;
            }
        }
        default:
            throw new QueryStateException(
                "Unexpected operand type in comparison operator: " + tc0);
        }
    }

    static void compareMaps(
        RuntimeControlBlock rcb,
        MapValue v0,
        MapValue v1,
        FuncCode opCode,
        CompResult res,
        Location location) {

        if (v0.size() != v1.size()) {
            res.comp = 1;
            return;
        }

        for (Map.Entry<String, FieldValue> e0 : v0.getMap().entrySet()) {

            String k0 = e0.getKey();
            FieldValue fv0 = e0.getValue();
            FieldValue fv1 = v1.get(k0);

            if (fv1 == null) {
                res.comp = 1;
                return;
            }

            compare(rcb, fv0, fv1, opCode, res, location);

            if (res.comp != 0 || res.haveNull || res.incompatible) {
                return;
            }
        }

        res.comp = 0;
        return;
    }

    @Override
    protected void displayContent(
        StringBuilder sb,
        QueryFormatter formatter) {

        displayInputIter(sb, formatter, theLeftOp);
        displayInputIter(sb, formatter, theRightOp);
    }

    @Override
    void displayName(StringBuilder sb) {

        switch (theCode) {
        case OP_GT:
            sb.append("GREATER_THAN");
            break;
        case OP_GE:
            sb.append("GREATER_OR_EQUAL");
            break;
        case OP_LT:
            sb.append("LESS_THAN");
            break;
        case OP_LE:
            sb.append("LESS_OR_EQUAL");
            break;
        case OP_EQ:
            sb.append("EQUAL");
            break;
        case OP_NEQ:
            sb.append("NOT_EQUAL");
            break;
        default:
            break;
        }
    }
}
