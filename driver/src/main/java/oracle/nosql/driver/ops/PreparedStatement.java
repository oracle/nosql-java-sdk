/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import oracle.nosql.driver.query.PlanIter;
import oracle.nosql.driver.values.FieldValue;

/**
 * A class encapsulating a prepared query statement. It includes state that
 * can be sent to a server and executed without re-parsing the query. It
 * includes bind variables which may be set for each successive use of the
 * query. The prepared query itself is read-only but this object contains a
 * mutable map of bind variables and is not thread-safe if variables are
 * used.
 * <p>
 * A single instance of PreparedStatement is thread-safe if bind variables
 * are <em>not</em> used. If bind variables are to be used and the statement
 * shared among threads additional instances of PreparedStatement can be
 * constructed using {@link #copyStatement}.
 */
public class PreparedStatement {

    private final String sqlText;

    private final String queryPlan;

    private final String querySchema;

    /*
     * The serialized PreparedStatements created at the backend store. There is
     * one of them for each union branch (or a single one if the query has no
     * UNION). They are opaque for the driver. They are received from the proxy
     * and one of them (the one corresponding to the current UNION branch that
     * is being executed) is sent back to the proxy every time a new batch of
     * results is needed.
     */
    private final ArrayList<byte[]> proxyStatements;

    /*
     * The part of the query plan that must be executed at the driver. It is
     * received from the proxy when the query is prepared there. It is
     * deserialized by the driver and not sent back to the proxy again.
     * Applicable to advanced queries only.
     */
    private final PlanIter driverQueryPlan;

    /*
     * The number of registers required to run the full query plan.
     * Applicable to advanced queries only.
     */
    private final int numRegisters;

    /*
     * The number of iterators in the full query plan
     * Applicable to advanced queries only.
     */
    private final int numIterators;

    /*
     * Maps the name of each external variable to its id, which is a position
     * in a FieldValue array stored in the RuntimeControlBlock and holding the
     * values of the variables. Applicable to advanced queries only.
     */
    private final Map<String, Integer> variables;

    /*
     * The values for the external variables of the query. This map is
     * populated by the application. It is sent to the proxy every time a
     * new batch of results is needed. The values in this map are also
     * placed in the RuntimeControlBlock FieldValue array, just before the
     * query starts its execution at the driver.
     */
    private Map<String, FieldValue> boundVariables;


    /*
     * The namespaces returned from a prepared query result. One for each
     * UNION branch.
     */
    private final ArrayList<String> namespaces;


    /*
     * The top-table names returned from a prepared query result. One for
     * each UNION branch.
     */
    private final ArrayList<String> topTableNames;

    /*
     * the operation code for the query.
     */
    private final byte operation;

    /*
     * The maximum degree of parallelization possible for this query
     */
    private final int maxParallelism;

    /* The one operation code we care about */
    /* "5" == PrepareCallback.QueryOperation.SELECT */
    private final byte OPCODE_SELECT = 5;

    /**
     * Constructs a PreparedStatement. Construction is hidden to eliminate
     * application access to the underlying byte[], reducing the chance of
     * corruption.
     *
     * This is public so that it can be constructed on deserialization.
     * @param sqlText the query
     * @param queryPlan the query plan
     * @param querySchema the query schema
     * @param proxyStatement proxy statement
     * @param driverPlan the portion of the query plan executed on driver
     * @param numIterators num iterators in plan
     * @param numRegisters num registers in the plan
     * @param externalVars external variables for the query
     * @param namespace namespace, if any, from deserialization
     * @param tableName table name, if any, from deserialization
     * @param operation operation code for the query
     * @param maxParallelism the maximum degree of parallelism possible for the
     * query
     * @hidden
     */
    public PreparedStatement(
        String sqlText,
        String queryPlan,
        String querySchema,
        ArrayList<byte[]> proxyStatements,
        PlanIter driverPlan,
        int numIterators,
        int numRegisters,
        Map<String, Integer> externalVars,
        ArrayList<String> namespaces,
        ArrayList<String> tableNames,
        byte operation,
        int maxParallelism) {

        if (proxyStatements.isEmpty()) {
            throw new IllegalArgumentException(
                "Invalid prepared query: no proxy-side query");
        }

        this.sqlText = sqlText;
        this.queryPlan = queryPlan;
        this.querySchema = querySchema;
        this.proxyStatements = proxyStatements;
        this.driverQueryPlan = driverPlan;
        this.numIterators = numIterators;
        this.numRegisters = numRegisters;
        this.variables = externalVars;
        this.namespaces = namespaces;
        this.topTableNames = tableNames;
        this.operation = operation;
        this.maxParallelism = maxParallelism;
    }

    /**
     * Returns a new instance that shares this object's prepared query, which is
     * immutable, but does not share its variables.
     *
     * @return a new PreparedStatement using this instance's prepared query.
     * Bind variables are uninitialized.
     */
    public PreparedStatement copyStatement() {

        return new PreparedStatement(sqlText,
                                     queryPlan,
                                     querySchema,
                                     proxyStatements,
                                     driverQueryPlan,
                                     numIterators,
                                     numRegisters,
                                     variables,
                                     namespaces,
                                     topTableNames,
                                     operation,
                                     maxParallelism);
    }

    /**
     * Returns the SQL text of this PreparedStatement.
     *
     * @return the SQL text of this PreparedStatement.
     */
    public String getSQLText() {
        return sqlText;
    }

    /**
     * Returns a string representation of the query execution plan, if it was
     * requested in the {@link PrepareRequest}; null otherwise.
     *
     * @return the string representation of the query execution plan
     */
    public String getQueryPlan() {
        return queryPlan;
    }

    /**
     * Returns a string (JSON) representation of the schema of the query
     * result for this query, if it was
     * requested in the {@link PrepareRequest}; null otherwise.
     *
     * @return the string representation of the query schema
     *
     * @since 5.4
     */
    public String getQuerySchema() {
        return querySchema;
    }

    /**
     * Returns the map of variables to use for a prepared query with variables.
     *
     * @return the map, or null if there are no variables set.
     */
    public Map<String, FieldValue> getVariables() {
        return boundVariables;
    }

    /**
     * Internal use only
     * @return the bound variables
     * @hidden
     */
    public FieldValue[] getVariableValues() {

        if (boundVariables == null) {
            return null;
        }

        FieldValue[] values = new FieldValue[boundVariables.size()];

        for (Map.Entry<String, FieldValue> entry : boundVariables.entrySet()) {
            int varid = variables.get(entry.getKey());
            values[varid] = entry.getValue();
        }

        return values;
    }

    /**
     * Clears all bind variables from the statement.
     */
    public void clearVariables() {
        if (boundVariables != null) {
            boundVariables.clear();
        }
    }

    /**
     * Binds an external variable to a given value. The variable is identified
     * by its name.
     *
     * @param name the name of the variable
     * @param value the value
     * @return this
     */
    public PreparedStatement setVariable(String name, FieldValue value) {

        if (boundVariables == null) {
            boundVariables = new HashMap<String, FieldValue>();
        }

        if (variables != null && variables.get(name) == null) {
            throw new IllegalArgumentException(
                "The query does not contain the variable: " + name);
        }

        boundVariables.put(name, value);
        return this;
    }

    /**
     * Binds an external variable to a given value. The variable is identified
     * by its position within the query string. The variable that appears first
     * in the query text has position 1, the variable that appears second has
     * position 2 and so on.
     *
     * @param pos the position of the variable
     * @param value the value
     * @return this
     */
    public PreparedStatement setVariable(int pos, FieldValue value) {

        if (variables == null) {
            String name = "#" + pos;
            return setVariable(name, value);
        }

        int searchId = pos - 1;

        for (Map.Entry<String, Integer> entry : variables.entrySet()) {
            int id = entry.getValue();
            if (id == searchId) {
                return setVariable(entry.getKey(), value);
            }
        }

        throw new IllegalArgumentException(
            "There is no external variable at position " +  pos);
    }

    /**
     *
     * Returns the serialized query. The byte array returned is opaque to
     * applications and is interpreted by the server for query execution.
     *
     * @return the serialized query
     * @hidden
     */
    public final byte[] getProxyStatement(int branch) {
        return proxyStatements.get(branch);
    }

    /**
     * Returns the maximum degree of parallelism possible for execution of
     * this query using multiple independent callers operating on discrete
     * subsets of the data. A value of 0 means no parallelism is possible. A
     * value of 1 is legitimate, for example, if there is only one shard in
     * a store, but also means no parallelism is possible.
     * <p>
     * This value can be used in {@link QueryRequest} to cause a query to
     * participate in a coordinated parallel query operations, allowing
     * individual queries to operate on a subset of data in a table. This
     * enables the queries to operate in parallel from threads, processes, or
     * across machines.
     *
     * @return the value
     *
     * @since 5.4.18
     */
    public int getMaximumParallelism() {
        return maxParallelism;
    }

    /**
     * Internal use only
     * @return the driver portion of the query plan as a string
     * @hidden
     */
    public String printDriverPlan() {
        if (driverQueryPlan != null) {
            return driverQueryPlan.display();
        }
        return null;
    }

    /**
     * Internal use only
     * @return the driver portion of the query plan
     * @hidden
     */
    public PlanIter driverPlan() {
        return driverQueryPlan;
    }

    /**
     * Internal use only
     * @return true if the query is simple
     * @hidden
     */
    public boolean isSimpleQuery() {
        return driverQueryPlan == null;
    }

    /**
     * Internal use only
     * @return num registers
     * @hidden
     */
    public int numRegisters() {
        return numRegisters;
    }

    /**
     * Internal use only
     * @return num iterators
     * @hidden
     */
    public int numIterators() {
        return numIterators;
    }

    /**
     * Internal use only
     * @return namespace from prepared statement, if any
     * @hidden
     */
    public String getNamespace(int branch) {
        return namespaces.get(branch);
    }

    /**
     * Internal use only
     * @return table name from prepared statement, if any
     * @hidden
     */
    public String getTopTableName(int branch) {
        return topTableNames.get(branch);
    }

    /**
     * Internal use only
     * @return true if the query does writes
     * @hidden
     */
    public boolean doesWrites() {
        /* if it's not SELECT, it does writes */
        return operation != OPCODE_SELECT;
    }
}
