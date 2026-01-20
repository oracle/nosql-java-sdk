/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandleAsync;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.values.IntegerValue;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import org.reactivestreams.tck.TestEnvironment;
import org.reactivestreams.tck.flow.FlowPublisherVerification;
import org.testng.annotations.AfterClass;

import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.Flow;

/**
 * Tests to check for reactive stream specification TCK.
 * This runs set of tests to check {@link QueryPublisher} conforms to
 * reactive stream specification defined in
 * <a href="https://github.com/reactive-streams/reactive-streams-jvm">
 * reactive-streams-jvm</a>
 * <p>
 * This test is excluded from the test profiles and must be run standalone.
 * It can be run expectedly against cloudsim using below command
 * <p>
 * mvn test -Dtest=QueryPublisherVerifierTest \
 * -DargLine="-Dtest.endpoint=http://localhost:8080
 */
public class QueryPublisherVerifierTest extends FlowPublisherVerification<List<MapValue>> {
    private static NoSQLHandleAsync asyncHandle;
    private static final String tableName = "testPublisher";
    private static final String createDDL =
        String.format("CREATE TABLE IF NOT EXISTS %s " +
            "(id LONG, name String, PRIMARY KEY(id))", tableName);
    private static final String dropDDL =
        "DROP TABLE IF EXISTS " + tableName;
    private static final int rows = 100;

    public QueryPublisherVerifierTest() {
        super(new TestEnvironment(true));
    }

    @org.testng.annotations.BeforeClass
    public static void setup() {
        asyncHandle = createHandle();
        TableRequest createReq = new TableRequest()
            .setStatement(createDDL)
            .setTableLimits(new TableLimits(0, 0, 1,
                TableLimits.CapacityMode.ON_DEMAND));
        asyncHandle.doTableRequest(createReq, 5000, 1000).join();

        final String insert = "DECLARE $id long; $name string;" +
            "INSERT INTO testPublisher values($id, $name)";
        PrepareRequest prepareReq =
            new PrepareRequest().setStatement(insert);
        PrepareResult prepareRes = asyncHandle.prepare(prepareReq).join();
        PreparedStatement prepStmt = prepareRes.getPreparedStatement();
        for (int i = 1; i<= rows; i++) {
            prepStmt.setVariable("$id", new IntegerValue(i));
            prepStmt.setVariable("$name", new StringValue(i+""));
            QueryRequest qr = new QueryRequest().setPreparedStatement(prepStmt);
            asyncHandle.query(qr).join();
        }
    }

    @AfterClass
    public static void teardown() {
        asyncHandle.doTableRequest(new TableRequest().setStatement(dropDDL),
                5000, 1000).join();
        asyncHandle.close();
    }

    @Override
    public Flow.Publisher<List<MapValue>> createFlowPublisher(long l) {
        QueryRequest request = new QueryRequest()
            .setStatement("select * from " + tableName + " where id <= " + l)
            .setLimit(1);
        QueryPaginatorResult result = new QueryPaginatorResult(request, asyncHandle);
        return new QueryPublisher(result);
    }

    @Override
    public Flow.Publisher<List<MapValue>> createFailedFlowPublisher() {
        QueryRequest request = new QueryRequest().setStatement("select * from" +
                tableName).setTimeout(5);
        QueryPaginatorResult result = new QueryPaginatorResult(request, asyncHandle);
        QueryPublisher publisher = new QueryPublisher(result);
        publisher.subscribe(new Flow.Subscriber<>() {
            @Override
            public void onSubscribe(Flow.Subscription subscription) {}

            @Override
            public void onNext(List<MapValue> item) {}

            @Override
            public void onError(Throwable throwable) {}

            @Override
            public void onComplete() {}
        });
        return publisher;
    }

    @Override
    public long maxElementsFromPublisher() {
        return rows;
    }

    static NoSQLHandleAsync createHandle() {
        final String endpoint = System.getProperty("test.endpoint",
            "http://localhost:8080");
        AuthorizationProvider provider = new AuthorizationProvider() {
            @Override
            public String getAuthorizationString(Request request) {
                return "Bearer ID";
            }

            @Override
            public CompletableFuture<String> getAuthorizationStringAsync(
                    Request request) {
                return CompletableFuture.completedFuture("Bearer ID");
            }

            @Override
            public void close() {

            }
        };
        NoSQLHandleConfig config = new NoSQLHandleConfig(endpoint, provider);
        return NoSQLHandleFactory.createNoSQLHandleAsync(config);
    }
}
