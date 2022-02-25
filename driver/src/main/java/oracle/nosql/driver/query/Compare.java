/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.Map;

import oracle.nosql.driver.values.ArrayValue;
import oracle.nosql.driver.values.BinaryValue;
import oracle.nosql.driver.values.BooleanValue;
import oracle.nosql.driver.values.DoubleValue;
import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.FieldValue.Type;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.LongValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.NumberValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.driver.values.TimestampValue;


public class Compare {

    static class CompResult {
        int comp;
        boolean incompatible;
        boolean haveNull;

        void clear() {
            comp = -1;
            incompatible = false;
            haveNull = false;
        }

        @Override
        public String toString() {
            return ("(comp, incompatible, haveNull) = (" +
                    comp + ", " + incompatible + ", " + haveNull + ")");
        }
    }

    static boolean equal(FieldValue v0, FieldValue v1) {

        if (v0.isNull()) {
            return v1.isNull();
        }

        if (v1.isNull()) {
            return false;
        }

        Type tc0 = v0.getType();
        Type tc1 = v1.getType();

        switch (tc0) {
        case ARRAY:
            if (tc1 != Type.ARRAY) {
                return false;
            }

            ArrayValue arr0 = (ArrayValue)v0;
            ArrayValue arr1 = (ArrayValue)v1;
            if (arr0.size() != arr1.size()) {
                return false;
            }

            for (int i = 0; i < arr0.size(); i++) {
                if (!equal(arr0.get(i), arr1.get(i))) {
                    return false;
                }
            }

            return true;
        case MAP:
            if (tc1 != Type.MAP) {
                return false;
            }

            MapValue map0 = (MapValue)v0;
            MapValue map1 = (MapValue)v1;
            if (map0.size() != map1.size()) {
                return false;
            }

            Iterator<String> keyIter = map0.getMap().keySet().iterator();

            while (keyIter.hasNext()) {
                String key0 = keyIter.next();
                FieldValue val1 = map1.get(key0);

                if (val1 == null) {
                    return false;
                }

                FieldValue val0 = map0.get(key0);

                if (!equal(val0, val1)) {
                    return false;
                }
            }

            return true;

        case INTEGER: {
            switch (tc1) {
            case INTEGER:
                return ((IntegerValue)v0).getValue() ==
                       ((IntegerValue)v1).getValue();
            case LONG:
                return ((IntegerValue)v0).getValue() ==
                       ((LongValue)v1).getValue();
            case DOUBLE:
                return ((IntegerValue)v0).getValue() ==
                       ((DoubleValue)v1).getValue();
            case NUMBER:
                BigDecimal bd0 = BigDecimal.
                                 valueOf(((IntegerValue)v0).getValue());
                BigDecimal bd1 = ((NumberValue)v1).getValue();
                return bd0.compareTo(bd1) == 0;
            default:
                return false;
            }
        }
        case LONG: {
            switch (tc1) {
            case INTEGER:
                return ((LongValue)v0).getValue() ==
                       ((IntegerValue)v1).getValue();
            case LONG:
                return ((LongValue)v0).getValue() ==
                       ((LongValue)v1).getValue();
            case DOUBLE:
                return ((LongValue)v0).getValue() ==
                       ((DoubleValue)v1).getValue();
            case NUMBER:
                BigDecimal bd0 = BigDecimal.
                                 valueOf(((LongValue)v0).getValue());
                BigDecimal bd1 = ((NumberValue)v1).getValue();
                return bd0.compareTo(bd1) == 0;
            default:
                return false;
            }
        }
        case DOUBLE: {
            switch (tc1) {
            case INTEGER:
                return ((DoubleValue)v0).getValue() ==
                       ((IntegerValue)v1).getValue();
            case LONG:
                return ((DoubleValue)v0).getValue() ==
                        ((LongValue)v1).getValue();
            case DOUBLE:
                return ((DoubleValue)v0).getValue() ==
                       ((DoubleValue)v1).getValue();
            case NUMBER:
                BigDecimal bd0 = BigDecimal.
                                 valueOf(((DoubleValue)v0).getValue());
                BigDecimal bd1 = ((NumberValue)v1).getValue();
                return bd0.compareTo(bd1) == 0;
            default:
                return false;
            }
        }
        case NUMBER: {
            NumberValue number = (NumberValue)v0;
            if (v1.isNumeric()) {
                return number.compareTo(v1) == 0;
            }
            return false;
        }
        case STRING: {
            if (tc1 == Type.STRING) {
                return ((StringValue)v0).getValue().equals(
                       ((StringValue)v1).getValue());
            }
            return false;
        }
        case BOOLEAN: {
            if (tc1 == Type.BOOLEAN) {
                return ((BooleanValue)v0).getValue() ==
                       ((BooleanValue)v1).getValue();
            }
            return false;
        }
        case TIMESTAMP: {
            if (tc1 == Type.TIMESTAMP) {
                return ((TimestampValue)v0).compareTo(v1) == 0;
            }
            return false;
        }
        case JSON_NULL: {
            assert(v0.isJsonNull());
            return v1.isJsonNull();
        }
        case EMPTY:
            return v1.isEMPTY();
        case BINARY:
            if (tc1 == Type.BINARY) {
                return ((BinaryValue)v0).compareTo(v1) == 0;
            }
            return false;
        default:
            throw new QueryStateException(
                "Unexpected operand type in equal operator: " + tc0);
        }
    }

    static int hashcode(FieldValue v) {

        if (v.isNull()) {
            return Integer.MAX_VALUE;
        }

        if (v.isJsonNull()) {
            return Integer.MIN_VALUE;
        }

        switch (v.getType()) {
        case ARRAY: {
            ArrayValue arr = (ArrayValue)v;
            int code = 1;
            for (int i = 0; i < arr.size(); ++i) {
                code = 31 * code + hashcode(arr.get(i));
            }

            return code;
        }
        case MAP: {
            MapValue map = (MapValue)v;
            int code = 1;
            for (Map.Entry<String, FieldValue> entry :
                 map.getMap().entrySet()) {
                code = (31 * code +
                        entry.getKey().hashCode() +
                        hashcode(entry.getValue()));
            }

            return code;
        }
        case INTEGER: {
            long l = ((IntegerValue)v).getValue();
            return (int)(l ^ (l >>> 32));
        }
        case LONG: {
            long l = ((LongValue)v).getValue();
            return (int)(l ^ (l >>> 32));
        }
        case DOUBLE: {
            double d = ((DoubleValue)v).getValue();
            long l = (long)d;
            if (d == l) {
                return (int)(l ^ (l >>> 32));
            }
            return Double.hashCode(d);
        }
        case NUMBER: {
            int code;
            long l;
            BigDecimal bd = ((NumberValue)v).getValue();
            try {
                l = bd.longValueExact();
            } catch (ArithmeticException e) {
                double d = bd.doubleValue();
                if (bd.compareTo(BigDecimal.valueOf(d)) == 0) {
                    code = Double.hashCode(d);
                    System.out.println("DBL hash value for number " + v + " = " + code);
                    return code;
                }
                code = v.hashCode();
                System.out.println("BD hash value for number " + v + " = " + code);
                return code;
            }

            code = (int)(l ^ (l >>> 32));
            System.out.println("INT hash value for number " + v + " = " + code);
            return code;
        }
        case STRING:
        case TIMESTAMP:
        case BINARY:
        case BOOLEAN:
            return v.hashCode();
        case EMPTY:
            return 0;
        default:
            throw new QueryStateException(
                "Unexpected value type in hashcode method: " + v.getType());
        }
    }

    /*
     * Compare 2 atomic values.
     *
     * The method throws an exception if either of the 2 values is non-atomic
     * or the values are not comparable. Otherwise, it retuns 0 if v0 == v1,
     * 1 if v0 > v1, or -1 if v0 < v1.
     *
     * Whether the 2 values are comparable depends on the "forSort" parameter.
     * If true, then values that would otherwise be considered non-comparable
     * are asusmed to have the following order:
     *
     * numerics < timestamps < strings < booleans < empty < json null < null 
     */
    static int compareAtomics(
        RuntimeControlBlock rcb,
        FieldValue v0,
        FieldValue v1,
        boolean forSort) {

        if (rcb.getTraceLevel() >= 4) {
            rcb.trace("Comparing values: \n" + v0 + "\n" + v1);
        }

        Type tc0 = v0.getType();
        Type tc1 = v1.getType();

        switch (tc0) {

        case NULL:
            if (forSort) {
                if (tc1 == Type.NULL) {
                    return 0;
                } else {
                    return 1;
                }
            }
            break;
        case JSON_NULL:
            if (tc1 == Type.JSON_NULL) {
                return 0;
            }
            if (forSort) {
                return (tc1 == Type.NULL ? -1 : 1);
            }
            break;
        case EMPTY:
            if (tc1 == Type.EMPTY) {
                return 0;
            }
            if (forSort) {
                return (tc1 == Type.NULL || tc1 == Type.JSON_NULL ? -1 : 1);
            }
            break;
        case INTEGER: {
            switch (tc1) {
            case INTEGER:
                return compareInts(((IntegerValue)v0).getValue(),
                                   ((IntegerValue)v1).getValue());
            case LONG:
                return compareLongs(((IntegerValue)v0).getValue(),
                                    ((LongValue)v1).getValue());
            case DOUBLE:
                return Double.compare(((IntegerValue)v0).getValue(),
                                      ((DoubleValue)v1).getValue());
            case NUMBER:
                return -v1.compareTo(v0);
            case STRING:
            case BOOLEAN:
            case TIMESTAMP:
            case NULL:
            case JSON_NULL:
            case EMPTY:
                if (forSort) {
                    return -1;
                }
                break;
            default:
                break;
            }
            break;
        }
        case LONG: {
            switch (tc1) {
            case INTEGER:
                return compareLongs(((LongValue)v0).getValue(),
                                    ((IntegerValue)v1).getValue());
            case LONG:
                return compareLongs(((LongValue)v0).getValue(),
                                    ((LongValue)v1).getValue());
            case DOUBLE:
                return Double.compare(((LongValue)v0).getValue(),
                                      ((DoubleValue)v1).getValue());
            case NUMBER:
                return -v1.compareTo(v0);
            case STRING:
            case BOOLEAN:
            case TIMESTAMP:
            case NULL:
            case JSON_NULL:
            case EMPTY:
                if (forSort) {
                    return -1;
                }
                break;
            default:
                break;
            }
            break;
        }
        case DOUBLE: {
            switch (tc1) {
            case INTEGER:
                return Double.compare(((DoubleValue)v0).getValue(),
                                      ((IntegerValue)v1).getValue());
            case LONG:
                return Double.compare(((DoubleValue)v0).getValue(),
                                      ((LongValue)v1).getValue());
            case DOUBLE:
                return Double.compare(((DoubleValue)v0).getValue(),
                                      ((DoubleValue)v1).getValue());
            case NUMBER:
                return -v1.compareTo(v0);
            case STRING:
            case BOOLEAN:
            case TIMESTAMP:
            case NULL:
            case JSON_NULL:
            case EMPTY:
                if (forSort) {
                    return -1;
                }
                break;
            default:
                break;
            }
            break;
        }
        case NUMBER: {
            NumberValue number = (NumberValue) v0;
            switch (tc1) {
            case INTEGER:
            case LONG:
            case DOUBLE:
            case NUMBER:
                return number.compareTo(v1);
            case STRING:
            case BOOLEAN:
            case TIMESTAMP:
            case NULL:
            case JSON_NULL:
            case EMPTY:
                if (forSort) {
                    return -1;
                }
                break;
            default:
                break;
            }
            break;
        }
        case TIMESTAMP: {
            switch (tc1) {
            case TIMESTAMP:
                return ((TimestampValue)v0).compareTo(v1);
            case INTEGER:
            case LONG:
            case DOUBLE:
            case NUMBER:
                if (forSort) {
                    return 1;
                }
                break;
            case STRING:
            case BOOLEAN:
            case NULL:
            case JSON_NULL:
            case EMPTY:
                if (forSort) {
                    return -1;
                }
                break;
            default:
                break;
            }
            break;
        }
        case STRING: {
            switch (tc1) {
            case STRING:
                return ((StringValue)v0).getValue().compareTo(
                       ((StringValue)v1).getValue());
            case INTEGER:
            case LONG:
            case DOUBLE:
            case NUMBER:
            case TIMESTAMP:
                if (forSort) {
                    return 1;
                }
                break;
            case BOOLEAN:
            case NULL:
            case JSON_NULL:
            case EMPTY:
                if (forSort) {
                    return -1;
                }
                break;
            default:
                break;
            }
            break;
        }
        case BOOLEAN: {
            switch (tc1) {
                case BOOLEAN:
                    return ((BooleanValue)v0).compareTo(v1);
            case INTEGER:
            case LONG:
            case DOUBLE:
            case NUMBER:
            case STRING:
            case TIMESTAMP:
                if (forSort) {
                    return 1;
                }
                break;
            case NULL:
            case JSON_NULL:
            case EMPTY:
                if (forSort) {
                    return -1;
                }
                break;
            default:
                break;
            }
            break;
        }
        default:
            break;
        }

        throw new QueryStateException(
            "Cannot compare value of type " + tc0 + " with value of type " + tc1);
    }

    static int sortAtomics(
        RuntimeControlBlock rcb,
        FieldValue v1,
        FieldValue v2,
        int sortPos,
        SortSpec[] sortSpecs) {

        if (v1.isNull()) {

            if (v2.isNull()) {
                return 0;
            }

            if (v2.isEMPTY() || v2.isJsonNull()) {
                return (sortSpecs[sortPos].theIsDesc ? -1 : 1);
            }

            return (sortSpecs[sortPos].theNullsFirst ? -1 : 1);
        }

        if (v2.isNull()) {

            if (v1.isEMPTY() || v1.isJsonNull()) {
                return (sortSpecs[sortPos].theIsDesc ? 1 : -1);
            }

            return (sortSpecs[sortPos].theNullsFirst ? 1 : -1);
        }

        if (v1.isEMPTY()) {

            if (v2.isEMPTY()) {
                return 0;
            }

            if (v2.isJsonNull()) {
                return (sortSpecs[sortPos].theIsDesc ? 1 : -1);
            }

            return (sortSpecs[sortPos].theNullsFirst ? -1 : 1);
        }

        if (v2.isEMPTY()) {

            if (v1.isJsonNull()) {
                return (sortSpecs[sortPos].theIsDesc ? -1 : 1);
            }

            return (sortSpecs[sortPos].theNullsFirst ? 1 : -1);
        }

        if (v1.isJsonNull()) {

            if (v2.isJsonNull()) {
                return 0;
            }

            return (sortSpecs[sortPos].theNullsFirst ? -1 : 1);
        }

        if (v2.isJsonNull()) {
            return (sortSpecs[sortPos].theNullsFirst ? 1 : -1);
        }

        int comp = compareAtomics(rcb, v1, v2, true);
        return (sortSpecs[sortPos].theIsDesc ? -comp : comp);
    }

    static int sortResults(
        RuntimeControlBlock rcb,
        MapValue r1,
        MapValue r2,
        String[] sortFields,
        SortSpec[] sortSpecs) {

        for (int i = 0; i < sortFields.length; ++i) {

            FieldValue v1 = r1.get(sortFields[i]);
            FieldValue v2 = r2.get(sortFields[i]);

            int comp = sortAtomics(rcb, v1, v2, i, sortSpecs);

            if (rcb.getTraceLevel() >= 3) {
                rcb.trace("Sort-Compared " + v1 + " with " + v2 +
                          " res = " + comp);
            }

            if (comp != 0) {
                return comp;
            }
        }

        /* they must be equal */
        return 0;
    }

    static int compareInts(int x, int y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }

    static int compareLongs(long x, long y) {
        return (x < y) ? -1 : ((x == y) ? 0 : 1);
    }
}
