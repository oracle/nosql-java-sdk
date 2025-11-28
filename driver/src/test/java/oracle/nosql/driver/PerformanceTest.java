/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import oracle.nosql.driver.http.Client;
import oracle.nosql.driver.http.NoSQLHandleAsyncImpl;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.Result;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.Semaphore;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertNotNull;

/**
 * Performance test for async APIs.
 * The test has two phases, warm-up and load phase
 * warm-up phase is to warm-up the netty connections
 * load phase to randomly run one of put, get, delete and query
 */
@Ignore("Performance test is too heavy to run as unit test")
public class PerformanceTest extends ProxyTestBase {
    private static final String table = "perf_test";
    private static final String ddl = "create table if not exists " + table +
            "(id long, name string, primary key(id))";
    private static final String dropDdl = "drop table if exists "+ table;
    private static final int WARMUP_OPS = 100;
    private static final int TOTAL_OPS = 100000;
    private static final int THREADS = 100;
    private static ExecutorService executor;

    private static final int pipelineDepth = 100;

    @BeforeClass
    public static void setupTest() {
        executor = Executors.newFixedThreadPool(THREADS);
    }
    @Before
    public void setup() {
        TableResult tres = tableOperationAsync(asyncHandle, ddl,
            new TableLimits(1000, 1000,1)).join();
        assertNotNull(tres.getTableName());
    }

    @After
    public void teardown() {
        TableResult tres =
            tableOperationAsync(asyncHandle, dropDdl, null).join();
        assertNotNull(tres.getTableName());
    }

    @Test
    public void test() throws Exception {
        Client client = ((NoSQLHandleAsyncImpl) asyncHandle).getClient();
        client.enableRateLimiting(true, 100);

        System.out.println("Warm-up phase");
        //runOpsAsync(WARMUP_OPS, pipelineDepth);
        runOpsAsync(WARMUP_OPS, pipelineDepth);

        StatsControl statsControl = asyncHandle.getStatsControl();
        statsControl.setProfile(StatsControl.Profile.ALL).setPrettyPrint(true);
        statsControl.start();


        System.out.println("Load phase");
        long start = System.nanoTime();
        //runOpsAsync(TOTAL_OPS, pipelineDepth);
        runOpsAsync(TOTAL_OPS, pipelineDepth);
        long end = System.nanoTime();


        Duration duration = Duration.ofNanos(end - start);
        double throughput = TOTAL_OPS / (duration.toMillis() / 1000.0);

        System.out.println("Completed " + TOTAL_OPS + " operations");
        System.out.println("Time = " + duration);
        System.out.println("Throughput = " + throughput + " ops/sec");
        statsControl.stop();
    }

    private void runOps(int count) throws Exception {
        List<CompletableFuture<Void>> futures = new ArrayList<>(count);
        Random random = new Random();
        AtomicInteger failures = new AtomicInteger();
        MapValue row = new MapValue()
            .put("id", 1)
            .put("name", "oracle");
        MapValue key = new MapValue().put("id", 1);

        for (int i = 0; i < count; i++) {
            futures.add(CompletableFuture.runAsync(() -> {
                try {
                    int op = random.nextInt(4);
                    switch (op) {
                        case 0 : {
                            //put op
                            PutRequest pr = new PutRequest()
                                .setTableName(table)
                                .setValue(row);
                            Result res = asyncHandle.put(pr).join();
                            assertNotNull(res);
                            break;
                        }
                        case 1 : {
                            GetRequest gr = new GetRequest()
                                .setTableName(table)
                                .setKey(key);
                            Result res = asyncHandle.get(gr).join();
                            assertNotNull(res);
                            break;
                        }
                        case 2 : {
                            DeleteRequest dr = new DeleteRequest()
                                .setTableName(table)
                                .setKey(key);
                            Result res = asyncHandle.delete(dr).join();
                            assertNotNull(res);
                            break;
                        } default : {
                            try(QueryRequest qr =
                                    new QueryRequest()
                                    .setStatement(
                                    "select * from " + table + " where id=1")) {
                                Result res =  asyncHandle.query(qr).join();
                                assertNotNull(res);
                            }
                        }
                    }
                } catch (Exception e) {
                    failures.incrementAndGet();
                }
            }, executor));
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        System.out.println("Failures = " + failures.get());
    }

    private void runOpsAsync(int count, int pipelineDepth) throws Exception {
        final Semaphore semaphore = new Semaphore(pipelineDepth);
        final List<CompletableFuture<Void>> futures = new ArrayList<>(count);
        Random random = new Random();
        AtomicInteger failures = new AtomicInteger();
        MapValue row = new MapValue()
            .put("id", 1)
            .put("name", "oracle");
        MapValue key = new MapValue().put("id", 1);

        for (int i = 0; i < count; i++) {
            try {
                semaphore.acquire();
                int op = random.nextInt(4);
                switch (op) {
                    case 0 : {
                        //put op
                        PutRequest pr = new PutRequest()
                            .setTableName(table)
                            .setValue(row);
                        CompletableFuture<PutResult> fut =
                            asyncHandle.put(pr).whenComplete((res, err) -> {
                            assertNotNull(res);
                            semaphore.release();
                        });
                        futures.add(fut.thenRun(() -> {}));
                        break;
                    }
                    case 1 : {
                        GetRequest gr = new GetRequest()
                            .setTableName(table)
                            .setKey(key);
                        CompletableFuture<GetResult> fut =
                            asyncHandle.get(gr).whenComplete((res, err) -> {
                            assertNotNull(res);
                            semaphore.release();
                        });
                        futures.add(fut.thenRun(() -> {}));
                        break;
                    }
                    case 2 : {
                        DeleteRequest dr = new DeleteRequest()
                            .setTableName(table)
                            .setKey(key);
                        CompletableFuture<DeleteResult> fut =
                            asyncHandle.delete(dr).whenComplete((res, err) -> {
                            assertNotNull(res);
                            semaphore.release();
                        });
                        futures.add(fut.thenRun(() -> {}));
                        break;
                    } default : {
                        try(QueryRequest qr =
                                new QueryRequest()
                                .setStatement(
                                "select * from " + table + " where id=1")) {
                            CompletableFuture<QueryResult> fut =
                                asyncHandle.query(qr)
                                    .whenComplete((res, err) -> {
                                        assertNotNull(res);
                                        semaphore.release();
                                    });
                            futures.add(fut.thenRun(() -> {}));
                        }
                    }
                }
            } catch (Exception e) {
                failures.incrementAndGet();
            }
        }
        CompletableFuture.allOf(futures.toArray(new CompletableFuture[0])).get();
        System.out.println("Failures = " + failures.get());
    }
}
