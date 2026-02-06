/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.query;

import java.io.PrintWriter;
import java.io.StringWriter;

/**
 * An internal class that encapsulates illegal states in the query engine.
 * The query engine operates inside clients and servers and cannot safely
 * throw IllegalStateException as that can crash the server. This exception is
 * used to indicate problems in the engine that are most likely query engine
 * bugs but are not otherwise fatal to the system.
 */
public class QueryStateException extends IllegalStateException {

    static final long serialVersionUID = 1;

    private final String stackTrace;

    public QueryStateException(String message) {
        super("Unexpected state in query engine:\n" + message);

        final StringWriter sw = new StringWriter(500);
        new RuntimeException().printStackTrace(new PrintWriter(sw));
        stackTrace = sw.toString();
    }

    @Override
    public String toString() {
        final StringBuilder sb = new StringBuilder(1000);
        sb.append(super.toString());
        sb.append("\nStack trace: ");
        sb.append(stackTrace);
        return sb.toString();
    }
}
