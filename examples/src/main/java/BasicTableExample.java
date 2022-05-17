/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.values.MapValue;

/**
 * A simple program to demonstrate basic table operation
 * - create a table
 * - put a row using put
 * - put a row using an insert query
 * - get a row
 * - query a table
 * - delete a row
 * - drop the table
 *
 * Examples can be run against:
 *  1. the cloud service
 *  2. the cloud simulator (CloudSim)
 *  3. the on-premise proxy and Oracle NoSQL Database instance, secure or
 *  not secure.
 *
 * To run:
 *   java -cp .:../lib/nosqldriver.jar BasicTableExample \
 *      <endpoint_or_region> [args]
 *
 * The endpoint and arguments vary with the environment. For details see
 * running instructions in Common.java
 */
public class BasicTableExample {

    public static void main(String[] args) {

        /* Validate arguments and get an authorization provider */
        Common setup = new Common("BasicTableExample");
        setup.validate(args);

        /* Set up the handle configuration */
        NoSQLHandleConfig config = new NoSQLHandleConfig(setup.getEndpoint());
        config.setAuthorizationProvider(setup.getAuthProvider());

        /*
         * Open the handle in a try-with-resources statement to ensure
         * proper closing of resources.
         */
        try (NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config))
        {

            /*
             * Create a simple table with an integer key and a single
             * json field
             */
            String tableName = "audienceData";
            final String createTableStatement =
                "CREATE TABLE IF NOT EXISTS " + tableName +
                "(cookie_id LONG, " +
                " audience_data JSON, " +
                " PRIMARY KEY(cookie_id))";

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

            /*
             * Construct a simple row, specifying the values for each
             * field. The value for the row is this:
             *
             * {
             *   "cookie_id": 123,
             *   "audience_data": {
             *     "ipaddr": "10.0.00.xxx",
             *     "audience_segment": {
             *        "sports_lover": "2018-11-30",
             *        "book_reader": "2018-12-01"
             *      }
             *   }
             * }
             */
            MapValue value = new MapValue()
                .put("cookie_id", 123) // fill in cookie_id field
                .put("audience_data",  // fill in audience_data field
                     "{\"ipaddr\" : \"10.0.00.xxx\"," +
                     " \"audience_segment\": { " +
                     "     \"sports_lover\" : \"2018-11-30\"," +
                     "     \"book_reader\" :  \"2018-12-01\"" +
                     "   }" +
                     " }");

            PutRequest putRequest = new PutRequest()
                .setValue(value)
                .setTableName(tableName);

            PutResult putRes = handle.put(putRequest);
            System.out.println("Put row: " + value + " result=" + putRes);

            /*
             * GET the row
             */
            MapValue key = new MapValue().put("cookie_id", 123);
            GetRequest getRequest = new GetRequest()
                .setKey(key)
                .setTableName(tableName);
            GetResult getRes = handle.get(getRequest);
            System.out.println("Got row: " + getRes.getValue());

            /*
             * Insert another row using an insert query. The values are in
             * order of the schema columns. The JSON column is inserted as JSON.
             */
            String insertQuery = "INSERT into " + tableName +
                " values(106, {" +
                "\"ipaddr\":\"10.0.00.xxx\", " +
                " \"audience_segment\": { " +
                " \"sports_lover\":\"2020-05-10\", " +
                " \"foodie\":\"2020-06-01\"}})";
            QueryRequest queryRequest = new QueryRequest();
            queryRequest.setStatement(insertQuery);
            try (QueryIterableResult results =
                handle.queryIterable(queryRequest)) {
                System.out.println("Inserted row via query, result:");
                for (MapValue qval : results) {
                    System.out.println("\t" + qval.toString());
                }
            }

            /*
             * PUT another row using JSON to enter the entire value
             */

            /* Construct a simple row */
            String jsonString = "{\"cookie_id\":456,\"audience_data\":" +
                "{\"ipaddr\":\"10.0.00.yyy\",\"audience_segment\":" +
                "{\"sports_lover\":\"2019-01-05\",\"foodie\":\"2018-12-31\"}}}";

            putRequest = new PutRequest()
                .setValueFromJson(jsonString, null) // no options
                .setTableName(tableName);
            handle.put(putRequest);
            System.out.println("Put row from json: " + jsonString);

            /*
             * GET the second row
             */
            key = new MapValue().put("cookie_id", 456);

            getRequest = new GetRequest()
                .setKey(key)
                .setTableName(tableName);
            getRes = handle.get(getRequest);
            System.out.println("Got second row: " + getRes.getValue());

            /*
             * QUERY the table. The table name is inferred from the
             * query statement.
             */
            String query = "SELECT * from " + tableName +
                " WHERE cookie_id = 456";

            queryRequest = new QueryRequest();
            queryRequest.setStatement(insertQuery);
            try (QueryIterableResult results =
                     handle.queryIterable(queryRequest)) {
                System.out.println("Query results for " + query + ": ");
                for (MapValue qval : results) {
                    System.out.println("\t" + qval.toString());
                }
            }
            /*
             * DELETE a row
             */
            DeleteRequest delRequest = new DeleteRequest()
                .setKey(key)
                .setTableName(tableName);

            DeleteResult del = handle.delete(delRequest);
            System.out.println("Deleted key " + key + " result=" + del);

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
            System.err.println("Problem seen: " + e);
        }
    }
}
