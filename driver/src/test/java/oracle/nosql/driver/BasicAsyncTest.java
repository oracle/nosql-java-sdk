/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.ListTablesRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableResult;
import oracle.nosql.driver.ops.TableUsageRequest;
import oracle.nosql.driver.values.MapValue;
import org.junit.Test;

import java.util.concurrent.CompletionException;

import static oracle.nosql.driver.util.BinaryProtocol.V4;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BasicAsyncTest extends ProxyTestBase {

    @Test
    public void smokeTest() {

        try {
            MapValue key = new MapValue().put("id", 10);
            MapValue value = new MapValue().put("id", 10).put("name", "jane");

            /* drop a table */
            tableOperationAsync(asyncHandle,
                                "drop table if exists testusers",
                                null)
                .whenComplete((tres, err) -> {
                    assertNotNull(tres.getTableName());
                    assertNull(tres.getTableLimits());
                })
            .thenCompose(ignored -> {
                /* drop again without if exists -- should throw */
                return tableOperationAsync(asyncHandle,
                    "drop table testusers",
                    null)
                .handle((tres, err) -> {
                    assertNotNull("operation should have thrown", err);
                    assertTrue(err instanceof CompletionException);
                    assertTrue("Expecting TableNotFoundException",
                        err.getCause() instanceof TableNotFoundException);
                    return null;
                });
            })
            .thenCompose(ignored -> {
                /* Create a table */
                return tableOperationAsync(
                    asyncHandle,
                    "create table if not exists testusers(id integer, " +
                    "name string, primary key(id))",
                    new TableLimits(500, 500, 50))
                .thenAccept(tres -> {
                    assertNotNull(tres);
                    assertEquals(TableResult.State.ACTIVE, tres.getTableState());
                });
            })
            .thenCompose(ignored -> {
                /* Create an index */
                return tableOperationAsync(
                    asyncHandle,
                    "create index if not exists Name on testusers(name)",
                    null)
                .thenAccept(tres -> {
                    assertNotNull(tres);
                    assertEquals(TableResult.State.ACTIVE, tres.getTableState());
                });
            })
            .thenCompose(ignored -> {
                /* list tables */
                ListTablesRequest listTables = new ListTablesRequest();
                return asyncHandle.listTables(listTables)
                    .thenApply(lres -> {
                        assertNotNull(lres);
                        /*
                         * the test cases don't yet clean up so there
                         *  may be additional tables present, be
                         * flexible in this assertion.
                         */
                        assertTrue(lres.getTables().length >= 1);
                        assertNotNull(lres.toString());
                        return lres;
                    });
            })
            .thenCompose(ignored -> {
                /* getTableUsage. It won't return much in test mode */
                if (!onprem) {
                    TableUsageRequest gtu = new TableUsageRequest()
                        .setTableName("testusers").setLimit(2)
                        .setEndTime(System.currentTimeMillis());
                    return asyncHandle.getTableUsage(gtu)
                        .thenAccept(gtuRes -> {
                            assertNotNull(gtuRes);
                            assertNotNull(gtuRes.getUsageRecords());
                        });
                }
                return null;
            })
            .thenCompose(ignored -> {
                /* PUT */
                PutRequest putRequest = new PutRequest()
                    .setValue(value)
                    .setTableName("testusers");
                return asyncHandle.put(putRequest)
                    .thenAccept(res -> {
                        assertNotNull(res.getVersion());
                        assertWriteKB(res);
                    });
            })
            .thenCompose(ignored -> {
                /* GET */
                GetRequest getRequest = new GetRequest()
                    .setKey(key)
                    .setTableName("testusers");

                return asyncHandle.get(getRequest)
                    .whenComplete((gres, err) -> {
                        assertNotNull(gres);
                        assertNotNull(gres.getJsonValue());
                        assertEquals("jane",
                            gres.getValue().getString("name"));
                        assertReadKB(gres);
                    });
            })
            .thenCompose(ignored -> {
                /* DELETE */
                DeleteRequest deleteRequest = new DeleteRequest()
                    .setKey(key)
                    .setTableName("testusers")
                    .setReturnRow(true);
                return asyncHandle.delete(deleteRequest)
                    .whenComplete((dres, err) -> {
                        assertNotNull(dres);
                        assertTrue(dres.getSuccess());
                        assertWriteKB(dres);
                        if (proxySerialVersion <= V4) {
                            assertNull(dres.getExistingVersion());
                        } else {
                            assertEquals(value, dres.getExistingValue());
                        }
                    });
            })
            .thenCompose(ignored -> {
                /* GET -- no row, it was removed above */
                GetRequest getRequest = new GetRequest()
                    .setTableName("testusers")
                    .setKey(key);
                return asyncHandle.get(getRequest)
                    .whenComplete((gres, err) -> {
                        assertNotNull(gres);
                        assertNull(gres.getValue());
                    });
            }).join();

            /* GET -- no table */
            GetRequest getRequest = new GetRequest()
                .setTableName("not_a_table")
                .setKey(key);
            asyncHandle.get(getRequest)
                .handle((gres, err) -> {
                    assertTrue(err instanceof CompletionException);
                    assertTrue(
                            "Attempt to access missing table should "
                                    + "have thrown",
                            err.getCause() instanceof TableNotFoundException);
                    return null;
                }).join();

            /* PUT -- invalid row -- this will throw */
            value.remove("id");
            value.put("not_a_field", 1);
            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName("testusers");
            asyncHandle.put(putRequest)
                .handle((pres, err) -> {
                    assertTrue(err instanceof CompletionException);
                    assertTrue(
                        "Attempt to put invalid row should have thrown",
                        err.getCause() instanceof IllegalArgumentException);
                    return null;
                }).join();
        } catch (Exception e) {
            e.printStackTrace();
            fail("Exception in test");
        }
    }
}
