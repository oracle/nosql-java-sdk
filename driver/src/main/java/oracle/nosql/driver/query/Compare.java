/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
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
                    return code;
                }
                code = v.hashCode();
                return code;
            }

            code = (int)(l ^ (l >>> 32));
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
     * Implements a total order among atomic values. The following order is
     * used among values that are not normally comparable with each other:
     *
     * numerics < timestamps < strings < booleans < binaries < empty < json null < null 
     */
    static int compareAtomicsTotalOrder(
        RuntimeControlBlock rcb,
        FieldValue v0,
        FieldValue v1) {

        if (rcb.getTraceLevel() >= 4) {
            rcb.trace("Comparing values: \n" + v0 + "\n" + v1);
        }

        Type tc0 = v0.getType();
        Type tc1 = v1.getType();

        switch (tc0) {

        case NULL:
            if (tc1 == Type.NULL) {
                return 0;
            } else {
                return 1;
            }
        case JSON_NULL:
            if (tc1 == Type.JSON_NULL) {
                return 0;
            }
            return (tc1 == Type.NULL ? -1 : 1);
        case EMPTY:
            if (tc1 == Type.EMPTY) {
                return 0;
            }
            return (tc1 == Type.NULL || tc1 == Type.JSON_NULL ? -1 : 1);
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
            default:
                return -1;
            }
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
            default:
                return -1;
            }
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
            default:
                return -1;
            }
        }
        case NUMBER: {
            NumberValue number = (NumberValue) v0;
            switch (tc1) {
            case INTEGER:
            case LONG:
            case DOUBLE:
            case NUMBER:
                return number.compareTo(v1);
            default:
                return -1;
            }
        }
        case TIMESTAMP: {
            switch (tc1) {
            case TIMESTAMP:
                return ((TimestampValue)v0).compareTo(v1);
            case INTEGER:
            case LONG:
            case DOUBLE:
            case NUMBER:
                return 1;
            default:
                return -1;
            }
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
                return 1;
            default:
                return -1;
            }
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
                return 1;
            default:
                return -1;
            }
        }
        case BINARY: {
             switch (tc1) {
                case BINARY:
                    return ((BinaryValue)v0).compareTo(v1);
             case INTEGER:
             case LONG:
             case DOUBLE:
             case NUMBER:
             case STRING:
             case TIMESTAMP:
             case BOOLEAN:
                 return 1;
             default:
                 return -1;
             }
        }
        default:
            throw new QueryStateException("Unexpected value type: " + tc0);
        }
    }

    static int compareAtomicsTotalOrder(
        RuntimeControlBlock rcb,
        FieldValue v1,
        FieldValue v2,
        SortSpec sortSpec) {

        int comp = compareAtomicsTotalOrder(rcb, v1, v2);

        comp = (sortSpec.theIsDesc ? -comp : comp);

        if (!sortSpec.theIsDesc && sortSpec.theNullsFirst) {

            if (v1.isSpecialValue() && !v2.isSpecialValue()) {
                comp = -1;
            }

            if (!v1.isSpecialValue() && v2.isSpecialValue()) {
                comp = 1;
            }
        } else if (sortSpec.theIsDesc && !sortSpec.theNullsFirst) {

            if (v1.isSpecialValue() && !v2.isSpecialValue()) {
                comp = 1;
            }

            if (!v1.isSpecialValue() && v2.isSpecialValue()) {
                comp = -1;
            }
        }

        return comp;
    }

    /*
     * Implements a total order among all kinds of values
     */
    static int compareTotalOrder(
        RuntimeControlBlock rcb,
        FieldValue v1,
        FieldValue v2,
        SortSpec sortSpec) {

        FieldValue.Type tc1 = v1.getType();
        FieldValue.Type tc2 = v2.getType();

        switch (tc1) {
        case MAP:
            switch (tc2) {
            case MAP:
                return compareMaps(rcb, (MapValue)v1, (MapValue)v2, sortSpec);
            case ARRAY:
                return (sortSpec.theIsDesc ? 1 : -1);
            default:
                return (sortSpec.theIsDesc ? -1 : 1);
            }
        case ARRAY:
            switch (tc2) {
            case MAP:
                return (sortSpec.theIsDesc ? -1 : 1);
            case ARRAY:
                return compareArrays(rcb, (ArrayValue)v1, (ArrayValue)v2,
                                     sortSpec);
            default:
                return (sortSpec.theIsDesc ? -1 : 1);
            }
        default:
            switch (tc2) {
            case MAP:
            case ARRAY:
                return (sortSpec.theIsDesc ? 1 : -1);
            default:
                return compareAtomicsTotalOrder(rcb, v1, v2, sortSpec);
            }
        }
    }

    static int compareMaps(
        RuntimeControlBlock rcb,
        MapValue v1,
        MapValue v2,
        SortSpec sortSpec) {

        SortSpec innerSortSpec = sortSpec;
        if (sortSpec.theIsDesc || sortSpec.theNullsFirst) {
            innerSortSpec = new SortSpec();
        }

        Iterator<String> keysIter1 = v1.sortedKeys().iterator();
        Iterator<String> keysIter2 = v2.sortedKeys().iterator();

        int comp = 0;

        while (keysIter1.hasNext() && keysIter2.hasNext()) {

            String k1 = keysIter1.next();
            String k2 = keysIter2.next();

            comp = k1.compareTo(k2);

            if (comp != 0) {
                return (sortSpec.theIsDesc ? -comp : comp);
            }

            comp = compareTotalOrder(rcb, v1.get(k1), v2.get(k2),
                                     innerSortSpec);

            if (comp != 0) {
                return (sortSpec.theIsDesc ? -comp : comp);
            }
        }

        if (v1.size() == v2.size()) {
            return 0;
        }

        if (keysIter2.hasNext()) {
            return (sortSpec.theIsDesc ? 1 : -1);
        }

        return (sortSpec.theIsDesc ? -1 : 1);
    }

    static int compareArrays(
        RuntimeControlBlock rcb,
        ArrayValue v1,
        ArrayValue v2,
        SortSpec sortSpec) {

        SortSpec innerSortSpec = sortSpec;
        if (sortSpec.theIsDesc || sortSpec.theNullsFirst) {
            innerSortSpec = new SortSpec();
        }

        Iterator<FieldValue> iter1 = v1.iterator();
        Iterator<FieldValue> iter2 = v2.iterator();

        while (iter1.hasNext() && iter2.hasNext()) {

            FieldValue e1 = iter1.next();
            FieldValue e2 = iter2.next();

            int comp = compareTotalOrder(rcb, e1, e2, innerSortSpec);

            if (comp != 0) {
                return (sortSpec.theIsDesc ? -comp : comp);
            }
        }

        if (v1.size() == v2.size()) {
            return 0;
        }

        if (iter2.hasNext()) {
            return (sortSpec.theIsDesc ? 1 : -1);
        }

        return (sortSpec.theIsDesc ? -1 : 1);
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

            int comp = compareAtomicsTotalOrder(rcb, v1, v2, sortSpecs[i]);

            if (rcb.getTraceLevel() >= 4) {
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
