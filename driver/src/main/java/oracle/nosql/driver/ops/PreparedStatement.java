/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.util.HashMap;
import java.util.Map;

import oracle.nosql.driver.query.PlanIter;
import oracle.nosql.driver.query.TopologyInfo;
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

    /*
     * Applicable to advanced queries only.
     */
    private volatile TopologyInfo topologyInfo;

    /*
     * The serialized PreparedStatement created at the backend store. It is
     * opaque for the driver. It is received from the proxy and sent back to
     * the proxy every time a new batch of results is needed.
     */
    private final byte[] proxyStatement;

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

    /**
     * @hidden
     * Constructs a PreparedStatement. Construction is hidden to eliminate
     * application access to the underlying byte[], reducing the chance of
     * corruption.
     *
     * This is public so that it can be constructed on deserialization.
     * @param sqlText the query
     * @param queryPlan the query plan
     * @param ti the topo info
     * @param proxyStatement proxy statement
     * @param driverPlan the portion of the query plan executed on driver
     * @param numIterators num iterators in plan
     * @param numRegisters num registers in the plan
     * @param externalVars external variables for the query
     */
    public PreparedStatement(
        String sqlText,
        String queryPlan,
        TopologyInfo ti,
        byte[] proxyStatement,
        PlanIter driverPlan,
        int numIterators,
        int numRegisters,
        Map<String, Integer> externalVars) {

        /* 10 is arbitrary. TODO: put magic number in it for validation? */
        if (proxyStatement == null || proxyStatement.length < 10) {
            throw new IllegalArgumentException(
                "Invalid prepared query, cannot be null");
        }

        this.sqlText = sqlText;
        this.queryPlan = queryPlan;
        this.topologyInfo = ti;
        this.proxyStatement = proxyStatement;
        this.driverQueryPlan = driverPlan;
        this.numIterators = numIterators;
        this.numRegisters = numRegisters;
        this.variables = externalVars;
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
                                     topologyInfo,
                                     proxyStatement,
                                     driverQueryPlan,
                                     numIterators,
                                     numRegisters,
                                     variables);
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
     * Returns the map of variables to use for a prepared query with variables.
     *
     * @return the map, or null if there are no variables set.
     */
    public Map<String, FieldValue> getVariables() {
        return boundVariables;
    }

    /**
     * @hidden
     * @return the bound variables
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
     * Sets the named variable in the map of variables to use for the query.
     * Existing variables with the same name are silently overwritten. The
     * names and types are validated when the query is executed.
     *
     * @param name the variable name used in the query statement
     *
     * @param value the value
     *
     * @return this
     */
    public PreparedStatement setVariable(String name, FieldValue value) {

        if (boundVariables == null) {
            boundVariables = new HashMap<String, FieldValue>();
        }

        if (variables != null && variables.get(name) == null) {
            throw new IllegalArgumentException(
                "The query doesn't contain the variable: " + name);
        }

        boundVariables.put(name, value);
        return this;
    }

    /**
     * @hidden
     *
     * Returns the serialized query. The byte array returned is opaque to
     * applications and is interpreted by the server for query execution.
     *
     * @return the serialized query
     */
    public final byte[] getStatement() {
        return proxyStatement;
    }

    /**
     * @hidden
     * @return the driver portion of the query plan as a string
     */
    public String printDriverPlan() {
        return driverQueryPlan.display();
    }

    /**
     * @hidden
     * @return the driver portion of the query plan
     */
    public PlanIter driverPlan() {
        return driverQueryPlan;
    }

    /**
     * @hidden
     * @return true if the query is simple
     */
    public boolean isSimpleQuery() {
        return driverQueryPlan == null;
    }

    /**
     * @hidden
     * @return num registers
     */
    public int numRegisters() {
        return numRegisters;
    }

    /**
     * @hidden
     * @return num iterators
     */
    public int numIterators() {
        return numIterators;
    }

    /**
     * @hidden
     * @return topo seq num
     */
    public synchronized int topologySeqNum() {
        return (topologyInfo == null ? -1 : topologyInfo.getSeqNum());
    }

    /**
     * @hidden
     * @param ti the topo info
     * @return this
     */
    public synchronized PreparedStatement setTopologyInfo(TopologyInfo ti) {

        if (ti == null) {
            return this;
        }

        if (topologyInfo == null) {
            topologyInfo = ti;
            return this;
        }

        if (topologyInfo.getSeqNum() < ti.getSeqNum()) {
            topologyInfo = ti;
        }
        return this;
    }

    /**
     * @hidden
     * @return top info
     */
    public TopologyInfo topologyInfo() {
        return topologyInfo;
    }
}
