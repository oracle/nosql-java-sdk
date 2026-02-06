/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.io.PrintStream;
import java.math.BigDecimal;
import java.math.MathContext;
import java.util.Map;
import java.util.TreeMap;

import oracle.nosql.driver.Consistency;
import oracle.nosql.driver.Durability;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.serde.Serializer;
import oracle.nosql.driver.ops.serde.SerializerFactory;
import oracle.nosql.driver.query.QueryDriver;
import oracle.nosql.driver.query.VirtualScan;
import oracle.nosql.driver.values.JsonUtils;

/**
 * A request that represents a query. A query may be specified as either a
 * textual SQL statement (a String) or a prepared query (an instance of
 * {@link PreparedStatement}), which may include bind variables.
 * <p>
 * For performance reasons prepared queries are preferred for queries that
 * may be reused. This is because prepared queries bypass query compilation.
 * They also allow for parameterized queries using bind variables.
 * <p>
 * There are two ways to get the results of a query: using an iterator or
 * loop through partial results.
 * <p>
 * <b>Iterator</b>
 * <p>
 * Use {@link NoSQLHandle#queryIterable(QueryRequest)} to get an iterable
 * that contains all the results. Usage example:
 * <pre>
 *    NoSQLHandle handle = ...;
 *
 *    try (
 *        QueryRequest qreq = new QueryRequest().setStatement("select * from foo");
 *        QueryIterableResult qir = handle.queryIterable(qreq)) {
 *        for( MapValue row : qir) {
 *            // do something with row
 *        }
 *    }
 * </pre>
 * <p>
 * <b>Partial results</b>
 * <p>
 * To compute and retrieve the full result set of a query, the same QueryRequest
 * instance will, in general, have to be executed multiple times (via
 * {@link NoSQLHandle#query}. Each execution returns a {@link QueryResult},
 * which contains a subset of the result set. The following code snippet
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
 * of the rows read satisfied the query conditions).
 * <p>
 * If an application wishes to terminate query execution before retrieving all
 * the query results, it should call {@link #close} in order to release any
 * local resources held by the query. This also allows the application to reuse
 * the QueryRequest instance to run the same query from the beginning or a
 * different query.
 * <p>
 * <p>
 * <b>Parallel queries</b>
 * <p>
 * By default a query request operates over all of the rows in a table. Large
 * scale analytics, such as Apache Spark, Apache Hadoop, and others benefit
 * from the ability to split a query into multiple workloads, where each
 * workload operates on a distinct subset of a table's rows. This feature is
 * called "parallel query" and is enabled in this driver as of release 5.4.18.
 * The feature also requires a server that supports the feature. A server that
 * does not support the feature will always return 0 for the maximum amount
 * of parallelism mentioned below.
 * <p>
 * Parallel query allows an application to create a group of participating
 * queries that operate in their own threads, processes, or machines. Each
 * participant query declares itself to be N of M cooperating queries, where
 * N is the individual operation and M is the total number of cooperating
 * queries. The maximum value for M is returned in the
 * {@link PreparedStatement} returned in a {@link PrepareResult}. This
 * feature works best for queries that operate over the entire set of data or
 * a simple subset constrained by a predicate. Parallel queries cannot be
 * performed on aggregations and sorted queries. Sample code to use this
 * feature looks like this
 * <pre>
 * NoSQLHandle handle = ...;
 * PreparedStatement ps = handle.prepare(new PrepareRequest().setStatement(
 *     "select * from foo")).getPreparedStatement();
 * int maxParallelism = ps.getMaximumParallelism();
 *
 * // at this point up to maxParallelism queries can be operated independently,
 * // each one operating on a subset of data. The total number of cooperating
 * // queries can be up to maxParallelism, but smaller numbers are fine. The
 * // system decides how to partition the queries across unique rows
 *
 * // Consider a maxParallism of 100 and 10 cooperating threads or processes
 * // are desired. This is an example of what number 3 out of 10 would do to
 * // execute a cooperating query
 *
 * QueryRequest qreq = new QueryRequest().setPreparedStatement(ps).
 *   setNumberOfOperations(10)  // total number of ops cooperating
 *   setOperationNumber(3)      // this is number 3 of 10
 * </pre>
 *
 * The query would then be executed as normal but instead of operating over
 * the entire table the query will operate only on approximately 1/10 of the
 * data. It is up to the application to aggregate and process results from
 * cooperating queries. If they are to be run in separate threads or processes
 * it is up to the application to create those runtime entities as well.
 * <p>
 * QueryRequest instances are not thread-safe. That is, if two or more
 * application threads need to run the same query concurrently, they must
 * create and use their own QueryRequest instances.
 *
 * @see NoSQLHandle#queryIterable(QueryRequest)
 * @see NoSQLHandle#query(QueryRequest)
 * @see NoSQLHandle#prepare(PrepareRequest)
 */
public class QueryRequest extends DurableRequest implements AutoCloseable {

    private int traceLevel;

    private int limit;

    private int maxReadKB;

    private int maxWriteKB;

    private long maxMemoryConsumption = 1024 * 1024 * 1024;

    private long maxServerMemoryConsumption = 10 * 1024 * 1024;

    private MathContext mathContext = MathContext.DECIMAL32;

    private Consistency consistency;

    private String lastWriteMetadata;

    private String statement;

    private PreparedStatement preparedStatement;

    private byte[] continuationKey;

    private VirtualScan virtualScan;

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

    private String queryName;

    private boolean logFileTracing;

    private String driverQueryTrace;

    private Map<String, String> serverQueryTraces;

    private int batchCounter;

    private boolean inTestMode;

    /* these next 2 are related to parallel queries */
    private int operationNumber;

    private int numberOfOperations;

    /**
     * Default constructor for QueryRequest
     */
    public QueryRequest() {
    }

    /**
     * Creates an internal QueryRequest out of the application-provided request.
     * @return a copy of the instance in a new object
     * @hidden
     */
    public QueryRequest copyInternal() {

        QueryRequest internalReq = new QueryRequest();
        super.copyTo(internalReq);

        internalReq.traceLevel = traceLevel;
        internalReq.logFileTracing = logFileTracing;
        internalReq.queryName = queryName;
        internalReq.batchCounter = batchCounter;
        internalReq.limit = limit;
        internalReq.maxReadKB = maxReadKB;
        internalReq.maxWriteKB = maxWriteKB;
        internalReq.maxMemoryConsumption = maxMemoryConsumption;
        internalReq.maxServerMemoryConsumption = maxServerMemoryConsumption;
        internalReq.mathContext = mathContext;
        internalReq.consistency = consistency;
        internalReq.lastWriteMetadata = lastWriteMetadata;
        internalReq.preparedStatement = preparedStatement;
        internalReq.isInternal = true;
        internalReq.driver = driver;
        internalReq.topoSeqNum = topoSeqNum;
        internalReq.inTestMode = inTestMode;
        internalReq.operationNumber = operationNumber;
        internalReq.numberOfOperations = numberOfOperations;
        return internalReq;
    }

    /**
     * Creates a copy that starts fresh from the beginning.
     * @return a copy of the instance in a new object
     * @hidden
     */
    public QueryRequest copy() {
        QueryRequest internalReq = copyInternal();
        internalReq.statement = statement;
        internalReq.lastWriteMetadata = lastWriteMetadata;
        internalReq.isInternal = false;
        internalReq.shardId = -1;
        internalReq.driver = null;
        driverQueryTrace = null;
        batchCounter = 0;
        return internalReq;
    }

    /**
     * internal use only
     * @return the internal QueryDriver instance
     * @hidden
     */
    public QueryDriver getDriver() {
        return driver;
    }

    /**
     * internal use only
     *
     * @param driver an internal QueryDriver instance
     * @hidden
     */
    public void setDriver(QueryDriver driver) {

        if (this.driver != null) {
            throw new IllegalArgumentException(
                "QueryRequest is already bound to a QueryDriver");
        }

        this.driver = driver;
    }

    /**
     * internal use only
     *
     * @return true if there is a QueryDriver instance
     * @hidden
     */
    public boolean hasDriver() {
        return driver != null;
    }

    /**
     * internal use only
     * @return true if the query has been prepared
     * @hidden
     */
    public boolean isPrepared() {
        return preparedStatement != null;
    }

    /**
     * internal use only
     * @return true if the query is a simple query
     * @hidden
     */
    public boolean isSimpleQuery() {
        return preparedStatement.isSimpleQuery();
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
     * internal use only
     * @return the shard id
     * @hidden
     */
    public int getShardId() {
        return shardId;
    }

    /**
     * internal use only
     * @param id the shard id
     * @hidden
     */
    public void setShardId(int id) {
        shardId = id;
    }

    /**
     * internal use only
     * @param vs the virtual scan
     * @hidden
     */
    public void setVirtualScan(VirtualScan vs) {
        virtualScan = vs;
    }

    /**
     * internal use only
     * @return the virtual scan
     * @hidden
     */
    public VirtualScan getVirtualScan() {
        return virtualScan;
    }

    /**
     * internal use only
     * @param level trace level
     * @return this
     * @hidden
     */
    public QueryRequest setTraceLevel(int level) {

        if (level > 32) {
            throw new IllegalArgumentException("trace level must be <= 32");
        }
        traceLevel = level;
        return this;
    }

    /**
     * internal use only
     * @return trace level
     * @hidden
     */
    public int getTraceLevel() {
        return traceLevel;
    }

    /**
     * Set a symbolic name for this query. This name will appear in query logs
     * if query tracing has been turned on.
     *
     * @param name the query name
     *
     * @return this
     * @hidden
     */
    public QueryRequest setQueryName(String name) {
        queryName = name;
        return this;
    }

    /**
     * Returns the query name
     *
     * @return the query name, or null if it has not been set
     * @hidden
     */
    public String getQueryName() {
        return queryName;
    }

    /**
     * If the logFileTracing parameter is set to true, log records produced
     * during query execution tracing will be written to the log files.
     * Otherwise, they are shipped by the servers to the driver, where they
     * can be displayed via the {@link #printTrace} method.
     *
     * @param value tracing log files setting
     * @return this
     * @hidden
     */
    public QueryRequest setLogFileTracing(boolean value) {
        logFileTracing = value;
        return this;
    }

    /**
     * internal use only
     * @return if log file tracing is enabled
     * @hidden
     */
    public boolean getLogFileTracing() {
        return logFileTracing;
    }

    /**
     * internal use only
     * @param traces the query traces to add
     * @hidden
     */
    public void addQueryTraces(Map<String, String> traces) {

        if (traces == null) {
            return;
        }

        if (serverQueryTraces == null) {
            serverQueryTraces = new TreeMap<String, String>();
        }
        serverQueryTraces.putAll(traces);
    }

    /**
     * internal use only
     * @param out the stream to print to
     * @hidden
     */
    public void printTrace(PrintStream out) {

        StringBuilder sb = new StringBuilder();

        sb.append("\n\n---------------------------------\n");
        sb.append("CLIENT : " + queryName);
        sb.append("\n---------------------------------\n\n");
        if (driver != null) {
            sb.append(driver.getQueryTrace());
        } else if (driverQueryTrace != null) {
            sb.append(driverQueryTrace);
        }
        sb.append("\n");

        if (serverQueryTraces != null) {
            for (Map.Entry<String, String> entry : serverQueryTraces.entrySet()) {
                sb.append("\n\n-------------------------------------------\n");
                sb.append(queryName);
                sb.append(": ");
                sb.append(entry.getKey());
                sb.append("\n-------------------------------------------\n\n");
                sb.append(entry.getValue());
                sb.append("\n");
            }
        }

        out.println(sb.toString());
    }

    /**
     * internal use only
     * @return the current batch counter
     * @hidden
     */
    public int getBatchCounter() {
        return batchCounter;
    }

    /**
     * Increment the current batch counter
     * @hidden
     */
    public void incBatchCounter() {
        ++batchCounter;
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
     * not set by the application this value will be 0 which means no limit set.
     *
     * For update query with on-premise service, returns the update limit on
     * the number of records that can be updated in single update query. If not
     * set by the application this value will be 0 which means no application
     * limit set.
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
     * For update query, if with on-premise service, this is to set the update
     * limit on the number of records that can be updated in single query, if
     * not set by the application, default service limit is used. If with cloud
     * service, this update limit will be ignored, the maximum of records that
     * can be updated is limited by other cloud limits maxWriteKB and maxReadKB.
     *
     * @param limit the limit in terms of number of items returned, or the
     * maximum of records that can be updated in a update query.
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
     * memory. If the maximum amount of memory is exceeded, a exception will
     * be throw.
     * <p>
     * The default value is 1GB.
     *
     * @param maxBytes the amount of memory to use, in bytes
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
     * <p>
     * The default value is 1GB.
     *
     * @return the maximum number of memory bytes
     */
    public long getMaxMemoryConsumption() {
        return maxMemoryConsumption;
    }

    /**
     * On-premises only.
     *
     * Sets the maximum number of memory bytes that may be consumed by an
     * individual server node while servicing a query request.
     *
     * @param maxBytes the value to use in bytes
     *
     * @return this
     * @hidden
     */
    public QueryRequest setMaxServerMemoryConsumption(long maxBytes) {
        if (maxBytes < 0) {
            throw new IllegalArgumentException("maxBytes must be >= 0");
        }
        maxServerMemoryConsumption = maxBytes;
        return this;
    }

    /**
     * internal use only
     * @return max server memory consumption
     * @hidden
     */
    public long getMaxServerMemoryConsumption() {
        return maxServerMemoryConsumption;
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
     * internal use only
     * @return the continuation key
     * @hidden
     */
    public byte[] getContKey() {
        return continuationKey;
    }

    /**
     * internal use only
     * @param continuationKey the key
     * @return this
     * @hidden
     */
    public QueryRequest setContKey(byte[] continuationKey) {

        this.continuationKey = continuationKey;

        if (driver != null && !isInternal && continuationKey == null) {
            driverQueryTrace = driver.getQueryTrace();
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
    @Override
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
     * Sets the durability to use for the operation.
     * On-premises only. This setting only applies if the query modifies
     * a row using an INSERT, UPSERT, or DELETE statement. If the query is
     * read-only it is ignored.
     *
     * @param durability the durability value. Set to null for
     * the default durability setting on the server.
     *
     * @return this
     *
     * @since 5.4.0
     */
    public QueryRequest setDurability(Durability durability) {
        setDurabilityInternal(durability);
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
     * Sets the write metadata to use for the operation. This setting is optional
     * and only applies if the query modifies or deletes any rows using an
     * INSERT, UPDATE, UPSERT or DELETE statement. If the query is read-only
     * this setting is ignored. This is an optional parameter.<p>
     *
     * Write metadata is associated to a certain version of a row. Any subsequent
     * write operation will use its own write metadata value. If not specified
     * null will be used by default.
     * NOTE that if you have previously written a record with metadata and a
     * subsequent write does not supply metadata, the metadata associated with
     * the row will be null. Therefore, if you wish to have metadata
     * associated with every write operation, you must supply a valid JSON
     * construct to this method.<p>
     *
     * @param lastWriteMetadata the write metadata, must be null or a valid JSON
     *    construct: object, array, string, number, true, false or null,
     *    otherwise an IllegalArgumentException is thrown.
     * @throws IllegalArgumentException if lastWriteMetadata not null and invalid
     *    JSON construct
     * @return this
     * @since 5.4.20
     */
    public QueryRequest setLastWriteMetadata(String lastWriteMetadata) {
        if (lastWriteMetadata == null) {
            this.lastWriteMetadata = null;
            return this;
        }

        JsonUtils.validateJsonConstruct(lastWriteMetadata);
        this.lastWriteMetadata = lastWriteMetadata;
        return this;
    }

    /**
     * Returns the write metadata set for this request, or null if not set.
     *
     * @return the write metadata
     * @since 5.4.20
     */
    @Override
    public String getLastWriteMetadata() {
        return lastWriteMetadata;
    }

    /**
     * Sets the request timeout value, in milliseconds. This overrides any
     * default value set with {@link NoSQLHandleConfig#setRequestTimeout}.
     * The value must be positive.
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
     * Sets the optional namespace.
     * On-premises only.
     *
     * This overrides any default value set with
     * {@link NoSQLHandleConfig#setDefaultNamespace}.
     * Note: if a namespace is specified in the table name in the SQL statement
     * (using the namespace:tablename format), that value will override this
     * setting.
     *
     * @param namespace the namespace to use for the operation
     *
     * @return this
     *
     * @since 5.4.10
     */
    public QueryRequest setNamespace(String namespace) {
        super.setNamespaceInternal(namespace);
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
     * Returns the total number of operations in a coordinated parallel
     * query operation or 0 if this is not a parallel query.
     * @return the number of operations
     * @since 5.4.18
     */
    public int getNumberOfOperations() {
        return numberOfOperations;
    }

    /**
     * Returns the individual operation number for this query if it is
     * participating in a coordinated parallel query operation or 0 if not.
     * The value is 1-based.
     * @return the operation number
     * @since 5.4.18
     */
    public int getOperationNumber() {
        return operationNumber;
    }

    /**
     * Sets the total number of operations in a coordinated parallel
     * query operation. This value will only be valid if the request
     * contains a prepared query and must be less than or equal to
     * the value returned by {@link PreparedStatement#getMaximumParallelism}.
     * Validation is performed during query execution.
     *
     * @param numberOfOperations the number of operations
     * @return this
     * @since 5.4.18
     */
    public QueryRequest setNumberOfOperations(int numberOfOperations) {
        this.numberOfOperations = numberOfOperations;
        return this;
    }

    /**
     * Sets the individual operation number for this query if it is
     * participating in a coordinated parallel query operation. This number
     * must be less than or equal to the total number of operations.
     * The operation number is 1-based and the value will only be valid if
     * the request contains a prepared query.
     * Validation is performed during query execution.
     *
     * @param operationNumber the operation number
     * @return this
     * @since 5.4.18
     */
    public QueryRequest setOperationNumber(int operationNumber) {
        this.operationNumber = operationNumber;
        return this;
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
     *
     * Return consistency if non-null. If null, return the default
     * Consistency from the config object
     * @hidden
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
        /*
         * Parallel queries have multiple requirements:
         * o only for prepared queries
         * o if set, both number of operations and op number need to be set
         * o operation number must be <= number of operations
         * o number of operations must be <= the max
         */

        /* only check one of the 2 params. The need for both is below */
        if (getNumberOfOperations() > 0) {
            if (!isPrepared()) {
                throw new IllegalArgumentException(
                    "Parallel queries are only allowed on prepared queries");
            }
            /* check both non-zero and value of operation number */
            if (getOperationNumber() > getNumberOfOperations() ||
                getOperationNumber() <= 0) {
                throw new IllegalArgumentException(
                    "Invalid parallel query operation number " +
                    getOperationNumber() + ", must be non-negative and <= " +
                    getNumberOfOperations());
            }
            /* check max */
            int max = getPreparedStatement().getMaximumParallelism();
            if (getNumberOfOperations() > max) {
                throw new IllegalArgumentException(
                    "Invalid parallel query number of operations " +
                    getNumberOfOperations() + ", must be <= to the maximum: " +
                    max);
            }
        } else if (getOperationNumber() != 0 ||
                   getNumberOfOperations() < 0) {
            throw new IllegalArgumentException(
                "Invalid parallel query operation number " +
                getOperationNumber() + ", both operation number and " +
                "number of operations must be non-negative");
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

    /**
     * internal use only
     * @param v the test mode
     * @hidden
     */
    public void setInTestMode(boolean v) {
        inTestMode = v;
    }

    /**
     * internal use only
     * @return the current test mode
     * @hidden
     */
    public boolean inTestMode() {
        return inTestMode;
    }
}
