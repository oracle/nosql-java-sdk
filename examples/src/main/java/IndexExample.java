/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

import java.util.List;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.values.MapValue;

/**
 * A simple program to demonstrate creation and use of an index.
 * - create a table
 * - create an index
 * - insert data
 * - read using the index
 * - drop the table
 *
 * Examples can be run against:
 *  1. the cloud service
 *  2. the cloud simulator (CloudSim)
 *  3. the on-premise proxy and Oracle NoSQL Database instance, secure or
 *  not secure.
 *
 * To run:
 *   java -cp .:../lib/nosqldriver.jar IndexExample \
 *      <endpoint_or_region> [args]
 *
 * The endpoint and arguments vary with the environment. For details see
 * running instructions in Common.java
 */
public class IndexExample {

    public static void main(String[] args) throws Exception {

        /* Validate arguments and get an authorization provider */
        Common setup = new Common("IndexExample");
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
             * Create a simple table with an integer key and a userInfo json
             * field
             */
            String tableName = "exampleProfiles";
            String createTableStatement =
                "CREATE TABLE IF NOT EXISTS "  + tableName +
                "(id INTEGER, " +
                "userInfo JSON," +
                "primary key(id))";

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
             * Create an index on the firstName field inside the
             * userInfo JSON object.
             */
            final String createIndexStatement =
                "CREATE INDEX IF NOT EXISTS idx1 ON " + tableName +
                "(userInfo.firstName AS STRING)";

            tableRequest =
                new TableRequest().setStatement(createIndexStatement);
            /* this call will succeed or throw an exception */
            handle.doTableRequest(tableRequest,
                                  60000, /* wait up to 60 sec */
                                  1000); /* poll once per second */
            System.out.println("Created index idx1");

            /*
             * Put rows in the table
             */

            /* construct a simple row */
            MapValue value = new MapValue().put("id", 1).
                putFromJson("userInfo",
                            "{\"firstName\":\"Taylor\"," +
                            "\"lastName\":\"Smith\"," +
                            "\"age\":33}",null);

            PutRequest putRequest = new PutRequest().setValue(value)
                .setTableName(tableName);
            PutResult putRes = handle.put(putRequest);
            System.out.println("Put result of " + value + " = " + putRes);

            /* construct a second row */
            value = new MapValue().put("id", 2).
                putFromJson("userInfo",
                            "{\"firstName\":\"Xiao\"," +
                             "\"lastName\":\"Zhu\"," +
                             "\"age\":17}",null);

            putRequest = new PutRequest().setValue(value)
                .setTableName(tableName);
            putRes = handle.put(putRequest);
            System.out.println("Put row: " + value);

            /* construct a third row */
            value = new MapValue().put("id", 3).
                putFromJson("userInfo",
                            "{\"firstName\":\"Supriya\"," +
                            "\"lastName\":\"Jain\"," +
                            "\"age\":65}",null);

            putRequest = new PutRequest().setValue(value)
                .setTableName(tableName);
            putRes = handle.put(putRequest);
            System.out.println("Put row: " + value);

            /* construct a fourth row */
            value = new MapValue().put("id", 4).
                putFromJson("userInfo",
                            "{\"firstName\":\"Taylor\"," +
                            "\"lastName\":\"Rodriguez\"," +
                            "\"age\":43}",null);

            putRequest = new PutRequest().setValue(value)
                .setTableName(tableName);
            putRes = handle.put(putRequest);
            System.out.println("Put row: " + value);

            /*
             * Query from the indexed field
             */
            String query =
                "SELECT * FROM " + tableName + " p WHERE " +
                "p.userInfo.firstName=\"Taylor\"";
            List<MapValue> results = Common.runQuery(handle,
                                                     query);

            /* Print the results */
            System.out.println("Number of query results: " + results.size());
            for (MapValue qval : results) {
                System.out.println("\t" + qval.toString());
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
            System.err.println("Problem seen: " + e);
        } finally {
            /* Shutdown handle so process can exit */
            handle.close();
        }
    }
}
