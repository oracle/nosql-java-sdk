/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.math.BigDecimal;

import oracle.nosql.driver.values.FieldValue;
import oracle.nosql.driver.values.FieldValue.Type;
import oracle.nosql.driver.values.NullValue;

class AggrIterState extends PlanIterState {

    long theCount;

    long theLongSum;

    double theDoubleSum;

    BigDecimal theNumberSum = null;

    Type theSumType = Type.LONG;

    boolean theNullInputOnly = true;

    FieldValue theMinMax = NullValue.getInstance();

    @Override
    public void reset(PlanIter iter) {
        super.reset(iter);
        theCount = 0;
        theLongSum = 0;
        theDoubleSum = 0;
        theNumberSum = null;
        theSumType = Type.LONG;
        theNullInputOnly = true;
        theMinMax = NullValue.getInstance();
    }
}
