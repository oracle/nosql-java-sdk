/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;


import oracle.nosql.driver.ops.AddReplicaRequest;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.DropReplicaRequest;
import oracle.nosql.driver.ops.GetIndexesRequest;
import oracle.nosql.driver.ops.GetIndexesResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.ListTablesResult;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.ReplicaStatsRequest;
import oracle.nosql.driver.ops.ReplicaStatsResult;
import oracle.nosql.driver.ops.SystemRequest;
import oracle.nosql.driver.ops.SystemResult;
import oracle.nosql.driver.ops.SystemStatusRequest;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.ops.TableUsageResult;
import oracle.nosql.driver.ops.WriteMultipleRequest;
import oracle.nosql.driver.ops.WriteMultipleResult;
import oracle.nosql.driver.values.MapValue;
import org.reactivestreams.Publisher;

import java.time.Duration;

public interface NoSQLHandleAsync extends AutoCloseable {
    Publisher<GetResult> get(GetRequest request);

    Publisher<PutResult> put(PutRequest request);

    Publisher<DeleteResult> delete(DeleteRequest request);
    Publisher<TableResult> getTable(GetTableRequest request);

    Publisher<TableResult> tableRequest(TableRequest request);
    Publisher<ListTablesResult> listTables(ListTablesRequest request);

    Publisher<QueryResult> query(QueryRequest request);

    Publisher<MapValue> queryIterable(QueryRequest request);

    Publisher<PrepareResult> prepare(PrepareRequest request);

    Publisher<WriteMultipleResult> writeMultiple(WriteMultipleRequest request);

    Publisher<MultiDeleteResult> multiDelete(MultiDeleteRequest request);

    Publisher<SystemResult> systemRequest(SystemRequest request);
    Publisher<SystemResult> systemStatus(SystemStatusRequest request);

    Publisher<TableUsageResult> getTableUsage(TableUsageRequest request);

    Publisher<GetIndexesResult> getIndexes(GetIndexesRequest request);

    Publisher<String> listNamespaces();

    Publisher<String> listRoles();

    Publisher<UserInfo> listUsers();

    Publisher<TableResult> doTableRequest(TableRequest request,
                                          Duration timeout,
                                          Duration pollInterval);
    Publisher<SystemResult> doSystemRequest(SystemRequest request,
                                 Duration timeout,
                                 Duration pollInterval);

    Publisher<TableResult> addReplica(AddReplicaRequest request);

    Publisher<TableResult> dropReplica(DropReplicaRequest request);

    Publisher<ReplicaStatsResult> getReplicaStats(ReplicaStatsRequest request);

    StatsControl getStatsControl();
}
