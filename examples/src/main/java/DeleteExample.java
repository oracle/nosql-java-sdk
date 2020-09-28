/*-
 * Copyright (c) 2011, 2020 Oracle and/or its affiliates.  All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

import java.util.List;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.MultiDeleteRequest;
import oracle.nosql.driver.ops.MultiDeleteResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.values.MapValue;

/**
 * A simple program to demonstrate deletion of rows.
 * - create a table
 * - put a row
 * - delete a row
 * - delete multiple row
 * - drop the table
 *
 * Examples can be run against:
 *  1. the cloud service
 *  2. the cloud simulator (CloudSim)
 *  3. the on-premise proxy and Oracle NoSQL Database instance, secure or
 *  not secure.
 *
 * To run:
 *   java -cp .:../lib/nosqldriver.jar DeleteExample \
 *      <endpoint_or_region> [args]
 * The endpoint and arguments vary with the environment. For details see
 * running instructions in Common.java
 */
public class DeleteExample {

    public static void main(String[] args) throws Exception {

        /* Validate arguments and get an authorization provider */
        Common setup = new Common("DeleteExample");
        setup.validate(args);

        /* Set up the handle configuration */
        NoSQLHandleConfig config = new NoSQLHandleConfig(setup.getEndpoint());
        config.setAuthorizationProvider(setup.getAuthProvider());

        /*
         * Open the handle
         */
        NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config);

        try {
            /*
             * Create a simple table
             */
            String tableName = "examplesAddress";
            String createTableStatement =
                "CREATE TABLE IF NOT EXISTS " + tableName +
                "(id INTEGER, " +
                " address_line1 STRING, " +
                " address_line2 STRING, " +
                " pin INTEGER, " +
                " PRIMARY KEY(SHARD(pin), id))";
            TableRequest tableRequest = new TableRequest()
                .setStatement(createTableStatement)
                .setTableLimits(new TableLimits(50, 50, 50));
            System.out.println("Creating table " + tableName);
            /* this call will succeed or throw an exception */
            handle.doTableRequest(tableRequest,
                                  60000, /* wait up to 60 sec */
                                  1000); /* poll once per second */
            System.out.println("Created table " + tableName);

            /*
             * PUT a row
             */

            /* construct a simple row */
            MapValue value = new MapValue().
                put("id", 1).put("pin",1234567).
                put("address_line1", "10 Red Street").
                put("address_line2", "Apt 3");

            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName(tableName);

            PutResult putRes = handle.put(putRequest);
            System.out.println("put result = " + putRes);

            /* construct a simple row */
            String jsonString =
                "{\"id\": 2,\"pin\": 1234567, "
                + "\"address_line1\":\"2 Green Street\","
                + "\"address_line2\":\"Suite 9\"}";

            putRequest = new PutRequest()
                .setValueFromJson(jsonString, null) // no options
                .setTableName(tableName);
            handle.put(putRequest);

            /* construct a row */
            jsonString =
                "{\"id\": 3,\"pin\": 1234567 ,"
                + "\"address_line1\":\"5 Blue Ave\","
                + "\"address_line2\":\"Floor 2\"}";

            putRequest = new PutRequest()
                .setValueFromJson(jsonString, null) // no options
                .setTableName(tableName);
            handle.put(putRequest);

            /* construct a row */
            jsonString =
                "{\"id\": 4, \"pin\": 87654321 , "
                + "\"address_line1\":\"9 Yellow Boulevard\","
                + "\"address_line2\":\"Apt 3\"}";

            putRequest = new PutRequest()
                .setValueFromJson(jsonString, null) // no options
                .setTableName(tableName);
            handle.put(putRequest);

            /* Select all the records */
            String query = "select * from " + tableName;
            List<MapValue> results = Common.runQuery(handle,
                                                     query);
            System.out.println("Number of rows in table: " + results.size());
            for (MapValue qval1 : results) {
                System.out.println("\t" + qval1.toString());
            }

            /*
             * Delete a single row - pass the entire key for
             * the row to be deleted
             */
            MapValue key = new MapValue();
            key.put("id", 2);
            key.put("pin", 1234567);

            DeleteRequest delRequest = new DeleteRequest()
                .setKey(key)
                .setTableName(tableName);

            DeleteResult del = handle.delete(delRequest);
            if (del.getSuccess()) {
                System.out.println("Delete succeed");
            }

            /*
             * Delete multiple rows -
             * userAddress table's primary key is <pin, id> where
             * the shard key is the pin.
             * to delete a range of id's that share the same shard key
             * one can pass the shard key to a MultiDeleteRequest
             */
            key = new MapValue().put("pin", 1234567);
            MultiDeleteRequest multiDelRequest = new MultiDeleteRequest()
                .setKey(key)
                .setTableName(tableName);

            MultiDeleteResult mRes = handle.multiDelete(multiDelRequest);
            System.out.println("MultiDelete result = " + mRes);

            /*
             * Query to verify that all related ids for a shard are deleted
             * The query is the same as above.
             */
            results = Common.runQuery(handle, query);

            System.out.println("Number of rows in table: " + results.size());
            for (MapValue qval1 : results) {
                System.out.println("\t" + qval1.toString());
            }

            /*
             * DROP the table
             */
            System.out.println("Dropping table " + tableName);
            tableRequest = new TableRequest()
                .setStatement("DROP TABLE IF EXISTS " + tableName);
            /* this call will succeed or throw an exception */
            handle.doTableRequest(tableRequest,
                                  60000, /* wait up to 60 sec */
                                  1000); /* poll once per second */
        } catch (Exception e) {
            System.err.println("Exception seen: " + e);
        } finally {
            /* Shutdown handle so process can exit */
            handle.close();
        }
    }
}
