/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

/**
 * A simple class to hold query expression and plan formatting information,
 * such as indent level. A new instance of this class is passed to
 * display() methods.
 *
 * theIndent:
 * The current number of space chars to be printed as indentation when
 * displaying the expression tree or the query execution plan.
 */
public class QueryFormatter {

    private final int theIndentIncrement;

    private int theIndent;

    public QueryFormatter(int increment) {
        theIndentIncrement = increment;
    }

    public QueryFormatter() {
        theIndentIncrement = 2;
    }

    public int getIndent() {
        return theIndent;
    }

    public int getIndentIncrement() {
        return theIndentIncrement;
    }

    public void setIndent(int v) {
        theIndent = v;
    }

    public void incIndent() {
        theIndent += theIndentIncrement;
    }

    public void decIndent() {
        theIndent -= theIndentIncrement;
    }

    public void indent(StringBuilder sb) {
        for (int i = 0; i < theIndent; ++i) {
            sb.append(' ');
        }
    }
}
