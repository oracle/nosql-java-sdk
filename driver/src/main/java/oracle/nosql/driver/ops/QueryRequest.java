/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.math.BigDecimal;
import java.math.MathContext;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.query.TopologyInfo;

/**
 * A request that represents a query. A query may be specified as either a
 * textual SQL statement (a String) or a prepared query (an instance of
 * {@link PreparedStatement}), which may include bind variables.
 * <p>
 * For performance reasons prepared queries are preferred for queries that
 * may be reused. This is because prepared queries bypass query compilation.
 * They also allow for parameterized queries using bind variables.
 * <p>
 * To compute and retrieve the full result set of a query, the same QueryRequest
 * instance will, in general, have to be executed multiple times (via
 * {@link NoSQLHandle#query}. Each execution returns a {@link QueryResult},
 * which contains a subset of the result set. The following code snipet
 * illustrates a typical query execution:
 * <pre>
 * NoSQLHandle handle = ...;
 *
 * QueryRequest qreq = new QueryRequest().setStatement("select * from foo");
 *
 * do {
 *   QueryResult qres = handle.query(qreq);
 *   List&lt;MapValue&gt; results = qres.getResults();
 *   // do something with the results
 * } while (!qreq.isDone())
 * </pre>
 * Notice that a batch of results returned by a QueryRequest execution
 * may be empty. This is because during each execution the query is allowed to
 * read or write a maximum number of bytes. If this maximum is reached, execution
 * stops. This can happen before any result was generated (for example, if none
 * of the rows read satified the query conditions).
 * <p>
 * If an application wishes to terminate query execution before retrieving all
 * of the query results, it should call {@link #close} in order to release any
 * local resources held by the query. This also allows the application to reuse
 * the QueryRequest instance to run the same query from the beginning or a
 * different query.
 * <p>
 * QueryRequest instances are not thread-safe. That is, if two or more
 * application threads need to run the same query concurrently, they must
 * create and use their own QueryRequest instances.
 *
 * @see NoSQLHandle#query
 * @see NoSQLHandle#prepare
 */
public class QueryRequest extends Request {

    private int traceLevel;

    private int limit;

    private int maxReadKB;

    private int maxWriteKB;

    private long maxMemoryConsumption = 1024 * 1024 * 1024;

    private MathContext mathContext = MathContext.DECIMAL32;

    private Consistency consistency;

    private String statement;

    private PreparedStatement preparedStatement;

    private byte[] continuationKey;

    /*
     * The QueryDriver, for advanced queries only.
     */
    private QueryDriver driver;

    /*
     * An "internal" request is one created and submitted for execution
     * by the ReceiveIter.
     */
    private boolean isInternal;

    /*
     * If shardId is >= 0, the QueryRequest should be executed only at the
     * shard with this id. This is the case only for advanced queries that
     * do sorting.
     */
    private int shardId = -1;

    public QueryRequest() {
    }

    /**
     * @hidden
     * Creates an internal QueryRequest out of the application-provided request.
     * @return a copy of the instance in a new object
     */
    public QueryRequest copyInternal() {

        QueryRequest internalReq = new QueryRequest();
        internalReq.timeoutMs = timeoutMs;
        internalReq.traceLevel = traceLevel;
        internalReq.limit = limit;
        internalReq.maxReadKB = maxReadKB;
        internalReq.maxWriteKB = maxWriteKB;
        internalReq.maxMemoryConsumption = maxMemoryConsumption;
        internalReq.mathContext = mathContext;
        internalReq.consistency = consistency;
        internalReq.preparedStatement = preparedStatement;
        internalReq.isInternal = true;
        internalReq.driver = driver;
        internalReq.tableName = tableName;
        return internalReq;
    }

    /**
     * @hidden
     *
     * @return the internal QueryDriver instance
     */
    public QueryDriver getDriver() {
        return driver;
    }

    /**
     * @hidden
     *
     * @param driver an internal QueryDriver instance
     */
    public void setDriver(QueryDriver driver) {

        if (this.driver != null) {
            throw new IllegalArgumentException(
                "QueryRequest is already bound to a QueryDriver");
        }

        this.driver = driver;
    }

    /**
     * @hidden
     *
     * @return true if there is a QueryDriver instance
     */
    public boolean hasDriver() {
        return driver != null;
    }

    /**
     * @hidden
     * @return true if the query has been prepared
     */
    public boolean isPrepared() {
        return preparedStatement != null;
    }

    /**
     * @hidden
     * @return true if the query is a simple query
     */
    public boolean isSimpleQuery() {
        return preparedStatement.isSimpleQuery();
    }

    /**
     * @hidden
     * @return TopologyInfo
     */
    public TopologyInfo topologyInfo() {
        return (preparedStatement == null ?
                null :
                preparedStatement.topologyInfo());
    }

    /**
     * @hidden
     * @return topology seq num
     */
    public int topologySeqNum() {
        return (preparedStatement == null ?
                -1 :
                preparedStatement.topologySeqNum());
    }

    /**
     * @hidden
     */
    @Override
    public boolean isQueryRequest() {
        return !isInternal;
    }

    /**
     * @hidden
     */
    @Override
    public boolean doesReads() {
        /*
         * Just about every permutation of query does reads
         */
        return true;
    }

    /**
     * @hidden
     */
    @Override
    public boolean doesWrites() {
        if (preparedStatement == null) {
            return false;
        }
        return preparedStatement.doesWrites();
    }

    /**
     * @hidden
     * @return the shard id
     */
    public int getShardId() {
        return shardId;
    }

    /**
     * @hidden
     * @param id the shard id
     */
    public void setShardId(int id) {
        shardId = id;
    }

    /**
     * @hidden
     *
     * @return trace level
     */
    public int getTraceLevel() {
        return traceLevel;
    }

    /**
     * @hidden
     *
     * @param level trace level
     * @return this
     */
    public QueryRequest setTraceLevel(int level) {

        if (level > 32) {
            throw new IllegalArgumentException("trace level must be <= 32");
        }
        traceLevel = level;
        return this;
    }

    /**
     * Cloud service only.
     * <p>
     * Sets the name or id of a compartment to be used for this operation.
     * <p>
     * The compartment may be specified as either a name (or path for nested
     * compartments) or as an id (OCID). A name (vs id) can only
     * be used when authenticated using a specific user identity. It is
     * <b>not</b> available if authenticated as an Instance Principal which can
     * be done when calling the service from a compute instance in the Oracle
     * Cloud Infrastructure.  See {@link
     * SignatureProvider#createWithInstancePrincipal}
     *
     * @param compartment the name or id. If using a nested compartment,
     * specify the full compartment path
     * <code>compartmentA.compartmentB</code>, but exclude the name of the
     * root compartment (tenant).
     *
     * @return this
     */
    public QueryRequest setCompartment(String compartment) {
        super.setCompartmentInternal(compartment);
        return this;
    }

    /**
     * Returns the limit on number of items returned by the operation. If
     * not set by the application this value will be 0 which means no limit.
     *
     * @return the limit, or 0 if not set
     */
    public int getLimit() {
        return limit;
    }

    /**
     * Sets the limit on number of items returned by the operation. This allows
     * an operation to return less than the default amount of data.
     *
     * @param limit the limit in terms of number of items returned
     *
     * @return this
     *
     * @throws IllegalArgumentException if the limit value is less than 0.
     */
    public QueryRequest setLimit(int limit) {
        if (limit < 0) {
            throw new IllegalArgumentException("limit must be >= 0");
        }
        this.limit = limit;
        return this;
    }

    /**
     * Returns the limit on the total data read during this operation, in
     * KB. If not set by the application this value will be 0 which means no
     * application-defined limit.
     *
     * @return the limit, or 0 if not set
     */
    public int getMaxReadKB() {
        return maxReadKB;
    }

    /**
     * Sets the limit on the total data read during this operation, in KB.
     * This value can only reduce the system defined limit. This limit is
     * independent of read units consumed by the operation.
     *
     * It is recommended that for tables with relatively low provisioned
     * read throughput that this limit be reduced to less than or equal to one
     * half of the provisioned throughput in order to avoid or reduce throttling
     * exceptions.
     *
     * @param maxReadKB the limit in terms of number of KB read during this
     * operation.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the maxReadKB value is less than 0
     */
    public QueryRequest setMaxReadKB(int maxReadKB) {
        if (maxReadKB < 0) {
            throw new IllegalArgumentException("maxReadKB must be >= 0");
        }
        this.maxReadKB = maxReadKB;
        return this;
    }

    /**
     * Returns the limit on the total data written during this operation, in
     * KB. If not set by the application this value will be 0 which means no
     * application-defined limit.
     *
     * @return the limit, or 0 if not set
     */
    public int getMaxWriteKB() {
        return maxWriteKB;
    }

    /**
     * Sets the limit on the total data written during this operation, in KB.
     * This limit is independent of write units consumed by the operation.
     *
     * @param maxWriteKB the limit in terms of number of KB written during this
     * operation.
     *
     * @return this
     *
     * @throws IllegalArgumentException if the maxWriteKB value is less than 0
     */
    public QueryRequest setMaxWriteKB(int maxWriteKB) {
        if (maxWriteKB < 0) {
            throw new IllegalArgumentException("maxWriteKB must be >= 0");
        }
        this.maxWriteKB = maxWriteKB;
        return this;
    }

    /**
     * Sets the maximum number of memory bytes that may be consumed by the
     * statement at the driver for operations such as duplicate elimination
     * (which may be required due to the use of an index on an array or map)
     * and sorting. Such operations may consume a lot of memory as they need
     * to cache the full result set or a large subset of it at the client
     * memory. The default value is 1GB.
     *
     * @param maxBytes the value to use in bytes
     *
     * @return this
     */
    public QueryRequest setMaxMemoryConsumption(long maxBytes) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be >= 0");
        }
        maxMemoryConsumption = maxBytes;
        return this;
    }

    /**
     * Returns the maximum number of memory bytes that may be consumed by the
     * statement at the driver for operations such as duplicate
     * elimination (which may be required due to the use of an index on an
     * array or map) and sorting (sorting by distance when a query contains
     * a geo_near() function). Such operations may consume a lot of memory
     * as they need to cache the full result set at the client memory.
     * The default value is 100MB.
     *
     * @return the maximum number of memory bytes
     */
    public long getMaxMemoryConsumption() {
        return maxMemoryConsumption;
    }

    /**
     * Returns the {@link MathContext} used for {@link BigDecimal} operations.
     * {@link MathContext#DECIMAL32} is used by default.
     *
     * @return the MathContext to use for the query
     */
    public MathContext getMathContext() {
        return mathContext;
    }

    /**
     * Sets the {@link MathContext} used for {@link BigDecimal} operations.
     * {@link MathContext#DECIMAL32} is used by default.
     *
     * @param mathContext the MathContext to use for the query
     * @return this
     */
    public QueryRequest setMathContext(MathContext mathContext) {

        if (mathContext == null) {
            throw new IllegalArgumentException("mathContext can not be null");
        }
        this.mathContext = mathContext;
        return this;
    }

    /**
     * Returns the query statement
     *
     * @return the statement, or null if it has not been set
     */
    public String getStatement() {
        return statement;
    }

    /**
     * Sets the query statement.
     *
     * @param statement the query statement
     *
     * @return this
     */
    public QueryRequest setStatement(String statement) {

        if (statement != null && preparedStatement != null &&
            !statement.equals(preparedStatement.getSQLText())) {
            throw new IllegalArgumentException(
                "The query text is not equal to the prepared one");
        }

        this.statement = statement;
        return this;
    }

    /**
     * Returns the prepared query statement
     *
     * @return the statement, or null if it has not been set
     */
    public PreparedStatement getPreparedStatement() {
        return preparedStatement;
    }

    /**
     * Sets the prepared query statement.
     *
     * @param preparedStatement the prepared query statement
     *
     * @return this
     */
    public QueryRequest setPreparedStatement(
        PreparedStatement preparedStatement) {

        if (statement != null && preparedStatement != null &&
            !statement.equals(preparedStatement.getSQLText())) {
            throw new IllegalArgumentException(
                "The query text is not equal to the prepared one");
        }

        this.preparedStatement = preparedStatement;
        return this;
    }

    /**
     * A convenience method to set the prepared query statement
     * from a PrepareResult
     *
     * @param prepareResult the result of a prepare request
     *
     * @return this
     */
    public QueryRequest setPreparedStatement(PrepareResult prepareResult) {

        this.preparedStatement = prepareResult.getPreparedStatement();
        return this;
    }

    /**
     * Returns the continuation key if set
     *
     * @return the key
     * @deprecated
     */
    @Deprecated
    public byte[] getContinuationKey() {
        return continuationKey;
    }

    /**
     * Sets the continuation key. This is used to continue an operation
     * that returned this key in its {@link QueryResult}.
     *
     * @param continuationKey the key which should have been obtained from
     * {@link QueryResult#getContinuationKey}
     *
     * @return this;
     * @deprecated There is no reason to use this method anymore, because
     * setting the continuation key is now done internally.
     */
    @Deprecated
    public QueryRequest setContinuationKey(byte[] continuationKey) {
        return setContKey(continuationKey);
    }

    /**
     * @hidden
     * @return the continuation key
     */
    public byte[] getContKey() {
        return continuationKey;
    }

    /**
     * @hidden
     * @param continuationKey the key
     * @return this
     */
    public QueryRequest setContKey(byte[] continuationKey) {

        this.continuationKey = continuationKey;

        if (driver != null && !isInternal && continuationKey == null) {
            driver.close();
            driver = null;
        }

        return this;
    }

    /**
     * Returns true if the query execution is finished, i.e., there are no
     * more query results to be generated. Otherwise false.
     *
     * @return whether the query is execution is finished or not
     */
    public boolean isDone() {
        return continuationKey == null;
    }

    /**
     * Terminates the query execution and releases any memory consumed by the
     * query at the driver. An application should use this method if it wishes
     * to terminate query execution before retrieving all of the query results.
     */
    public void close() {
        setContinuationKey(null);
    }

    /**
     * Sets the {@link Consistency} to use for the operation
     *
     * @param consistency the Consistency
     *
     * @return this
     */
    public QueryRequest setConsistency(Consistency consistency) {
        this.consistency = consistency;
        return this;
    }

    /**
     * Returns the consistency set for this request, or null if not set.
     *
     * @return the consistency
     */
    public Consistency getConsistency() {
        return consistency;
    }

    /**
     * Sets the request timeout value, in milliseconds. This overrides any
     * default value set in {@link NoSQLHandleConfig}. The value must be
     * positive.
     *
     * @param timeoutMs the timeout value, in milliseconds
     *
     * @return this
     *
     * @throws IllegalArgumentException if the timeout value is less than
     * or equal to 0
     */
    public QueryRequest setTimeout(int timeoutMs) {
        super.setTimeoutInternal(timeoutMs);
        return this;
    }

    /**
     * Returns the timeout to use for the operation, in milliseconds. A value
     * of 0 indicates that the timeout has not been set.
     *
     * @return the value
     */
    public int getTimeout() {
        return super.getTimeoutInternal();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createSerializer(SerializerFactory factory) {
        return factory.createQuerySerializer();
    }

    /**
     * @hidden
     */
    @Override
    public Serializer createDeserializer(SerializerFactory factory) {
        return factory.createQueryDeserializer();
    }

    @Override
    public String getTypeName() {
        return "Query";
    }

    /**
     * @hidden
     *
     * Return consistency if non-null. If null, return the default
     * Consistency from the config object
     */
    @Override
    public Request setDefaults(NoSQLHandleConfig config) {
        super.setDefaults(config);

        if (consistency == null) {
            consistency = config.getDefaultConsistency();
        }
        return this;
    }

    /**
     * @hidden
     */
    @Override
    public void validate() {
        if (statement == null && preparedStatement == null) {
            throw new IllegalArgumentException(
                "Either statement or prepared statement should be set");
        }
    }

    /**
     * @hidden
     */
    @Override
    public String getTableName() {
        if (preparedStatement == null) {
            return null;
        }
        return preparedStatement.getTableName();
    }

    /**
     * @hidden
     */
    @Override
    public boolean shouldRetry() {
        return false;
    }
}
