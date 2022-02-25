/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.IOException;

import oracle.nosql.driver.util.ByteInputStream;

/**
 * The order-by clause, for each sort expression allows for an optional
 * "sort spec", which specifies the relative order of NULLs (less than or
 * greater than all other values) and whether the values returned by the
 * sort expr should be sorted in ascending or descending order.
 *
 * The SortSpec class stores these two pieces of information.
 */
public class SortSpec {

    public final boolean theIsDesc;

    public final boolean theNullsFirst;

    public SortSpec(ByteInputStream in) throws IOException {
        theIsDesc = in.readBoolean();
        theNullsFirst = in.readBoolean();
    }
}
