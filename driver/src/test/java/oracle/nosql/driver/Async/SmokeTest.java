package oracle.nosql.driver.Async;

import oracle.nosql.driver.ProxyTestBase;
import oracle.nosql.driver.TableNotFoundException;
import oracle.nosql.driver.TimeToLive;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetTableRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.values.MapValue;
import org.junit.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class SmokeTest extends ProxyTestBase {
    final String table = "testusers";
    final String createDDL = "create table if not exists testusers(id " +
            "integer, " + "name string, primary key(id))";
    final String dropDDL = "drop table if exists testusers";
    final String idxDDL = "create index if not exists Name on testusers" +
            "(name)";
    final TableLimits limits = new TableLimits(500, 500, 50);

    final MapValue value = new MapValue().put("id", 10).put("name", "jane");

    @Test
    public void crudTest() {

        /* Drop table if already exists */
        StepVerifier.create(
            tableOperationAsync(asyncHandle, dropDDL, null))
            .assertNext(tres -> {
                assertNotNull(tres.getTableName());
                assertNull(tres.getTableLimits());
            })
            .verifyComplete();

        /* drop again without if exists -- should throw */
        StepVerifier.create(tableOperationAsync(asyncHandle, "drop table testusers",
            null))
            .expectError(TableNotFoundException.class)
            .verify();

        /* Create a table */
        StepVerifier.create(tableOperationAsync(asyncHandle, createDDL, limits))
            .assertNext(tres -> {
                assertEquals(TableResult.State.ACTIVE, tres.getTableState());
                assertEquals(table, tres.getTableName());
                if (onprem) {
                    assertNull(tres.getTableLimits());
                }
                else {
                    assertEquals(limits.toString(),
                            tres.getTableLimits().toString());
                }
            })
            .verifyComplete();

        /* Create an index */
        StepVerifier.create(tableOperationAsync(asyncHandle, idxDDL, null))
            .assertNext(tres ->
                assertEquals(TableResult.State.ACTIVE, tres.getTableState())
            )
            .verifyComplete();


        /* GetTableRequest for table that doesn't exist */
        GetTableRequest getTable = new GetTableRequest().setTableName("not_a_table");
        StepVerifier.create(asyncHandle.getTable(getTable))
            .expectError(TableNotFoundException.class)
            .verify();

        /* List tables */
        ListTablesRequest listTables = new ListTablesRequest();
        StepVerifier.create(asyncHandle.listTables(listTables),1)
            .assertNext(lres -> {
                assertTrue(lres.getTables().length >= 1);
                assertTrue(Arrays.stream(lres.getTables())
                    .anyMatch(s -> s.equalsIgnoreCase(table)));
            })
            .verifyComplete();

        /* PUT */
        PutRequest putRequest = new PutRequest().setValue(value)
            .setTableName(table);

        StepVerifier.create(asyncHandle.put(putRequest), 1)
            .assertNext(res ->  {
                assertNotNull(res.getVersion());
                assertWriteKB(res);
            })
            .thenRequest(1)
            .expectComplete()
            .verify();

        /* put a few more. set TTL to test that path. */
        for (int i=20;i<30;i++) {
            /* We can't share the PutRequest for async work. Need to create new
               PutRequest each time. This will not block this thread.
             */
            PutRequest pr = new PutRequest().setTableName(table)
                .setValue(new MapValue().put("id",i)
                    .put("name", "jane"))
                .setTTL(TimeToLive.ofHours(2));

            Mono.from(asyncHandle.put(pr)).delaySubscription(Duration.ofMillis(50)).subscribe(res -> {
                assertNotNull(res.getVersion());
                assertWriteKB(res);
            }, err ->
                fail("Not expecting put operation to fail " + err)
            );
        }

        /* sleep for sometime for above operations to complete */
        try {
            Thread.sleep(5000);
        } catch (InterruptedException ignored) {

        }


        /*
         * Test ReturnRow for simple put of a row that exists. 2 cases:
         * 1. unconditional (no return info)
         * 2. if absent (will return info)
         */
        PutRequest putAbsent = new PutRequest().setTableName(table)
            .setValue(new MapValue().put("id", 20).put("name", "jane1"))
            .setReturnRow(true);

        StepVerifier.create(asyncHandle.put(putAbsent))
            .assertNext(pr -> {
                assertNotNull(pr.getVersion()); /* success */
                assertNull(pr.getExistingVersion());
                assertNull(pr.getExistingValue());
            })
            .verifyComplete();

        putAbsent = new PutRequest().setTableName(table)
            .setValue(new MapValue().put("id", 20).put("name", "jane1"))
            .setReturnRow(true)
            .setOption(PutRequest.Option.IfAbsent);

        StepVerifier.create(asyncHandle.put(putAbsent))
            .assertNext(pr -> {
                assertNull(pr.getVersion()); /* failure */
                assertNotNull(pr.getExistingVersion());
                assertNotNull(pr.getExistingValue());
            })
            .verifyComplete();

        /* Get rows */
        for (int i=20;i<30;i++) {
            GetRequest getRequest = new GetRequest().setTableName(table)
                .setKey(new MapValue().put("id", i));
            StepVerifier.create(asyncHandle.get(getRequest))
            .assertNext(gr -> {
                assertNotNull(gr.getJsonValue());
                assertReadKB(gr);
            })
            .verifyComplete();
        }

        /* Delete a row */
        DeleteRequest deleteRequest = new DeleteRequest().setTableName(table)
                .setKey(new MapValue().put("id",20));
        StepVerifier.create(asyncHandle.delete(deleteRequest),1)
            .assertNext(dr -> {
                assertTrue(dr.getSuccess());
                assertWriteKB(dr);
            })
            .verifyComplete();

        /* GET -- no row, it was removed above */
        GetRequest notRequest = new GetRequest().setTableName(table)
            .setKey(new MapValue().put("id",20));
        StepVerifier.create(asyncHandle.get(notRequest),100)
            .assertNext(gr ->
                assertNull(gr.getValue())
            )
            .verifyComplete();

        /* GET -- no table */
        GetRequest noTblRequest = new GetRequest().setTableName("not_a_table")
            .setKey(new MapValue().put("ab", "cd"));
        StepVerifier.create(asyncHandle.get(noTblRequest))
            .expectError(TableNotFoundException.class)
            .verify();

        /* PUT -- invalid row -- this will throw */
        PutRequest invalidRequest = new PutRequest().setTableName(table)
            .setValue(new MapValue());
        StepVerifier.create(asyncHandle.put(invalidRequest))
            .expectError(IllegalArgumentException.class)
            .verify();
    }

    @Test
    public void queryTest() {
        /* Drop table if already exists */
        StepVerifier.create(
            tableOperationAsync(asyncHandle, dropDDL, null))
            .assertNext(tres -> {
                assertNotNull(tres.getTableName());
                assertNull(tres.getTableLimits());
            })
            .verifyComplete();

        /* create  a table */
        StepVerifier.create(tableOperationAsync(asyncHandle, createDDL, limits))
            .expectNextCount(1)
            .verifyComplete();

        /* put a few rows */
        for (int i=1;i<=20;i++) {
            /* We can't share the PutRequest for async work. Need to create new
               PutRequest each time. This will not block this thread.
             */
            PutRequest pr = new PutRequest().setTableName(table)
                    .setValue(new MapValue().put("id",i)
                            .put("name", "jane"))
                    .setTTL(TimeToLive.ofHours(2));

            Mono.from(asyncHandle.put(pr)).subscribe(res -> {
                assertNotNull(res.getVersion());
                assertWriteKB(res);
            }, err ->
                fail("Not expecting put operation to fail " + err)
            );
        }

        try {
            Thread.sleep(2000);
        } catch (InterruptedException ignored) {

        }
        /* Insert query */
        QueryRequest queryRequest = new QueryRequest().setStatement("insert " +
                "into " + table + " values(21,'jane')");
        StepVerifier.create(asyncHandle.query(queryRequest))
            .expectSubscription()
            .thenRequest(100)
            .assertNext(res -> {
                assertWriteKB(res);
                assertReadKB(res);
                assertEquals(1,res.getResults().size());
            })
            .verifyComplete();
        queryRequest.close();

        /* update query */
        QueryRequest updateRequest = new QueryRequest()
            .setStatement("update " + table + " t set t.name='jane1' " +
                    "where t.id=21");
        StepVerifier.create(asyncHandle.query(updateRequest),10)
            .assertNext(res -> {
                assertReadKB(res);
                assertWriteKB(res);
                assertEquals(1,res.getResults().size());
            })
            .verifyComplete();

        /* select query */
        QueryRequest selectRequest = new QueryRequest()
            .setStatement("select * from " + table);
        StepVerifier.create(asyncHandle.query(selectRequest))
            .assertNext(res -> {
                assertEquals(21, res.getResults().size());
                assertReadKB(res);
                // result comes in netty thread
                assertTrue(Thread.currentThread().getName().toLowerCase().contains(
                        "reactor"));
            })
            .verifyComplete();

        /* select query with back pressure */
        QueryRequest bpSelect = new QueryRequest()
            .setStatement("select * from " + table)
            .setLimit(10);
        StepVerifier.create(asyncHandle.query(bpSelect),1)
            .assertNext(res -> {
                assertEquals(10, res.getResults().size());
                assertTrue(Thread.currentThread().getName().toLowerCase().contains(
                        "reactor"));
            })
            .thenRequest(1)
            .assertNext(res -> {
                assertEquals(10, res.getResults().size());
                assertTrue(Thread.currentThread().getName().toLowerCase().contains(
                        "reactor"));
            })
            .thenRequest(1)
            .assertNext(res -> {
                assertEquals(1, res.getResults().size());
                assertTrue(Thread.currentThread().getName().toLowerCase().contains(
                        "reactor"));
            })
            .verifyComplete();

        /* select queryIterable */
        QueryRequest itrRequest = new QueryRequest()
            .setStatement("select * from " + table + " order by id limit 5");
        Flux<Integer> result =
                Flux.from(asyncHandle.queryIterable(itrRequest))
                    .map(mapValue -> mapValue.getInt("id"));
        StepVerifier.create(result)
            .assertNext(i -> {
                assertEquals(1, (int) i);
                assertTrue(Thread.currentThread().getName().toLowerCase().contains(
                        "bounded"));
            })
            .expectNext(2,3,4,5)
            .verifyComplete();
    }

}
