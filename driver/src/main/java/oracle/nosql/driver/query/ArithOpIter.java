/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;
import java.math.BigDecimal;

import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.FieldValue.Type;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.NullValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.util.ByteInputStream;
import oracle.nosql.driver.util.SerializationUtil;

/**
 * Iterator to implement the arithmetic operators
 *
 * any_atomic? ArithOp(any?, ....)
 *
 * An instance of this iterator implements either addition/substraction among
 * two or more input values, or multiplication/division among two or more input
 * values. For example, arg1 + arg2 - arg3 + arg4, or arg1 * arg2 * arg3 / arg4.
 *
 * The only arithmetic op that is strictly needed for the driver is the div
 * (real division) op, to compute an AVG aggregate function as the division of
 * a SUM by a COUNT. However, having all the arithmetic ops implemented allows
 * for expressions in the SELECT list that do arithmetic among aggregate
 * functions (for example: select a, sum(x) + sum(y) from foo group by a).
 */
public class ArithOpIter extends PlanIter {

    /**
     * Whether this iterator performs addition/substraction or
     * multiplication/division.
     */
    private final FuncCode theCode;

    private final PlanIter[] theArgs;

    /**
     * If theCode == FuncCode.OP_ADD_SUB, theOps is a string of "+" and/or "-"
     * chars, containing one such char per input value. For example, if the
     * arithmetic expression is (arg1 + arg2 - arg3 + arg4) theOps is "++-+".
     *
     * If theCode == FuncCode.OP_MULT_DIV, theOps is a string of "*", "/",
     * and/or "d" chars, containing one such char per input value. For example,
     * if the arithmetic expression is (arg1 * arg2 * arg3 / arg4) theOps
     * is "***\/". The "d" char is used for the div operator.
     */
    private final String theOps;

    private final transient int theInitResult;

    /*
     * Whether div is any of the operations to be performed by this ArithOpIter.
     */
    private final transient boolean theHaveRealDiv;

    public ArithOpIter(
        ByteInputStream in,
        short serialVersion) throws IOException {

        super(in, serialVersion);

        short ordinal = in.readShort();
        theCode = FuncCode.valueOf(ordinal);
        theArgs = deserializeIters(in, serialVersion);
        theOps = SerializationUtil.readString(in);

        theInitResult = (theCode == FuncCode.OP_ADD_SUB ? 0 : 1);
        theHaveRealDiv = theOps.contains("d");

        assert theOps.length() == theArgs.length :
            "Not enough operations: ops:" + (theOps.length() - 1) + " args:" +
            theArgs.length;
    }

    @Override
    public PlanIterKind getKind() {
        return PlanIterKind.ARITH_OP;
    }

    @Override
    FuncCode getFuncCode() {
        return theCode;
    }

    @Override
    public void open(RuntimeControlBlock rcb) {

        rcb.setState(theStatePos, new PlanIterState());
        for (PlanIter argIter : theArgs) {
            argIter.open(rcb);
        }
    }

    @Override
    public void reset(RuntimeControlBlock rcb) {

        for (PlanIter argIter : theArgs) {
            argIter.reset(rcb);
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

        for (PlanIter argIter : theArgs) {
            argIter.close(rcb);
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
         * Determine the type of the result for the expression by iterating
         * its components, enforcing the promotion rules for numeric types.
         *
         * Start with INTEGER, unless we have any div operator, in which case
         * start with DOUBLE.
         */
        Type resultType = (theHaveRealDiv ? Type.DOUBLE : Type.INTEGER);

        for (int i = 0; i < theArgs.length; i++) {

            PlanIter argIter = theArgs[i];
            boolean opNext = argIter.next(rcb);

            if (!opNext) {
                state.done();
                return false;
            }

            FieldValue argValue = rcb.getRegVal(argIter.getResultReg());

            if (argValue.isNull()) {
                FieldValue res = NullValue.getInstance();
                rcb.setRegVal(theResultReg, res);
                state.done();
                return true;
            }

            Type argType = argValue.getType();

            switch (argType) {
            case INTEGER:
                break;
            case LONG:
                if (resultType == Type.INTEGER) {
                    resultType = Type.LONG;
                }
                break;
            case DOUBLE:
                if (resultType == Type.INTEGER || resultType == Type.LONG) {
                    resultType = Type.DOUBLE;
                }
                break;
            case NUMBER:
                resultType = Type.NUMBER;
                break;
            default:
                throw new QueryException(
                    "Operand in arithmetic operation has illegal type\n" +
                    "Operand : " + i + " type :\n" +
                    argType, getLocation());
            }
        }

        int iRes = theInitResult;
        long lRes = theInitResult;
        double dRes = theInitResult;
        BigDecimal nRes = null;

        try {
            for (int i = 0 ; i < theArgs.length; i++) {

                PlanIter argIter = theArgs[i];
                FieldValue argValue = rcb.getRegVal(argIter.getResultReg());
                assert (argValue != null);

                if (theCode == FuncCode.OP_ADD_SUB) {
                    if (theOps.charAt(i) == '+') {
                        switch (resultType) {
                        case INTEGER:
                            iRes += argValue.getInt();
                            break;
                        case LONG:
                            lRes += argValue.getLong();
                            break;
                        case DOUBLE:
                            dRes += argValue.castAsDouble();
                            break;
                        case NUMBER:
                            if (nRes == null) {
                                nRes = argValue.getNumber();
                            } else {
                                nRes = nRes.add(argValue.getNumber(),
                                                rcb.getMathContext());
                            }
                            break;
                        default:
                            throw new QueryStateException(
                                "Invalid result type: " + resultType);
                        }
                    } else {
                        switch (resultType) {
                        case INTEGER:
                            iRes -= argValue.getInt();
                            break;
                        case LONG:
                            lRes -= argValue.getLong();
                            break;
                        case DOUBLE:
                            dRes -= argValue.castAsDouble();
                            break;
                        case NUMBER:
                            if (nRes == null) {
                                nRes = argValue.getNumber().negate();
                            } else {
                                nRes = nRes.subtract(argValue.getNumber(),
                                                     rcb.getMathContext());
                            }
                            break;
                        default:
                            throw new QueryStateException(
                                "Invalid result type: " + resultType);
                        }
                    }
                } else {
                    if (theOps.charAt(i) == '*') {
                        switch (resultType) {
                        case INTEGER:
                            iRes *= argValue.getInt();
                            break;
                        case LONG:
                            lRes *= argValue.getLong();
                            break;
                        case DOUBLE:
                            dRes *= argValue.castAsDouble();
                            break;
                        case NUMBER:
                            if (nRes == null) {
                                nRes = argValue.getNumber();
                            } else {
                                nRes = nRes.multiply(argValue.getNumber(),
                                                     rcb.getMathContext());
                            }
                            break;
                        default:
                            throw new QueryStateException(
                                "Invalid result type: " + resultType);
                        }
                    } else if (theOps.charAt(i) == '/') {
                        switch (resultType) {
                        case INTEGER:
                            iRes /= argValue.getInt();
                            break;
                        case LONG:
                            lRes /= argValue.getLong();
                            break;
                        case DOUBLE:
                            dRes /= argValue.castAsDouble();
                            break;
                        case NUMBER:
                            if (nRes == null) {
                                nRes = new BigDecimal(1);
                            }

                            nRes = nRes.divide(argValue.getNumber(),
                                               rcb.getMathContext());
                            break;
                        default:
                            throw new QueryStateException(
                                "Invalid result type: " + resultType);
                        }
                    } else {
                        switch (resultType) {
                        case DOUBLE:
                            dRes /= argValue.castAsDouble();
                            break;
                        case NUMBER:
                            if (nRes == null) {
                                nRes = new BigDecimal(1);
                            }

                            nRes = nRes.divide(argValue.getNumber(),
                                               rcb.getMathContext());
                            break;
                        default:
                            throw new QueryStateException(
                                "Invalid result type: " + resultType);
                        }
                    }
                }
            }
        } catch (ArithmeticException ae) {
            throw new QueryException(
                "Arithmetic exception in query: " + ae.getMessage(),
                ae, getLocation());
        }

        FieldValue res = null;
        switch (resultType) {
        case INTEGER:
            res = new IntegerValue(iRes);
            break;
        case LONG:
            res = new LongValue(lRes);
            break;
        case DOUBLE:
            res = new DoubleValue(dRes);
            break;
        case NUMBER:
            res = new NumberValue(nRes);
            break;
        default:
            throw new QueryStateException(
                "Invalid result type: " + resultType);
        }

        rcb.setRegVal(theResultReg, res);

        state.done();
        return true;
    }

    @Override
    protected void displayContent(StringBuilder sb, QueryFormatter formatter) {

        int i = 0;
        for (PlanIter argIter : theArgs) {

            formatter.indent(sb);
            if (theCode == FuncCode.OP_ADD_SUB) {
                if (theOps.charAt(i) == '+') {
                    sb.append('+');
                } else {
                    sb.append('-');
                }
            }
            else {
                if (theOps.charAt(i) == '*') {
                    sb.append('*');
                } else {
                    sb.append('/');
                }
            }
            sb.append(",\n");
            argIter.display(sb, formatter);
            if (i < theArgs.length - 1) {
                sb.append(",\n");
            }
            ++i;
        }
    }
}
