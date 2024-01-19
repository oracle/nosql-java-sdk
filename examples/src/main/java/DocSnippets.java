/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

import java.util.List;

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.DeleteResult;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PrepareRequest;
import oracle.nosql.driver.ops.PrepareResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.QueryIterableResult;
import oracle.nosql.driver.ops.QueryRequest;
import oracle.nosql.driver.ops.QueryResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.values.MapValue;
import oracle.nosql.driver.values.StringValue;
import oracle.nosql.driver.values.TimestampValue;

/**
 * These examples are used to verify documentation code snippets and should
 * not be included in an example package.
 */
public class DocSnippets {

    /**
     * Snippets to validate the doc on the "Use Tables in Java" page.
     *
     * Not actually executed.
     */
    static void docSnip() throws Exception {

        /*

         Comment regarding compartments. This is general information and
         not part of any example.

         A table is created in a compartment and it's name is scoped to that
         compartment. Unless otherwise specified, tables are managed in the
         root compartment of the tenancy. It is recommended
         that users create compartments for tables to organize them and
         better manage security rather than use the root compartment.

         There are several ways a compartment can be specified.
         1. Specify a default compartment in NoSQLHandleConfig which applies
         to all operations using the handle.
         2. Specify the compartment name or id (OCID) in each request in
         addition to specifying the table name. This overrides any
         default compartment.
         3. Specify the compartment name as a prefix on the table name. This
         overrides any default compartment as well as a compartment specified
         using API.

         An example of (2) is:
             GetRequest getReq = new GetRequest().setTableName("mytable")
                 .setCompartment("mycompartment");
         An example of (3) is:
             GetRequest getReq = new GetRequest()
                 .setTableName("mycompartment:mytable");

         All of the snippet examples assume that the compartment is defaulted
         and do not explicitly set a compartment for any individual request.

         */

        /* Snippet start  "Obtaining a NoSQL Handle" */

        /*
         * Configure a handle for the desired Region and AuthorizationProvider.
         *
         * By default this SignatureProvider constructor reads authorization
         * information from ~/.oci/config and uses the default user profile and
         * private key for request signing. Additional SignatureProvider
         * constructors are available if a config file is not available or
         * desirable.
         */
        AuthorizationProvider ap = new SignatureProvider();

        /*
         * Use the us-ashburn-1 region
         */
        NoSQLHandleConfig config =
            new NoSQLHandleConfig(Region.US_ASHBURN_1, ap);

        config.setAuthorizationProvider(ap);

        /*
         * Sets a default compartment for all requests from this handle. This
         * may be overridden in individual requests or by using a
         * compartment-name prefixed table name.
         */
        config.setDefaultCompartment("mycompartment");

        /*
         * Open the handle
         */
        NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config);

        /* Use the handle to execute operations */

        /* ------ Snippet end -------*/

        /* Snippet start  "Create Tables and Indexes" */

        /*
         * Create a simple table with an integer key and a single json data
         * field  and set your desired table capacity.
         * Set the table TTL value to 3 days.
         */
        String createTableDDL = "CREATE TABLE IF NOT EXISTS users " +
            "(id INTEGER, name STRING, " +
            "PRIMARY KEY(id)) USING TTL 3 days";
        TableLimits limits = new TableLimits(200, 100, 5);
        TableRequest tableRequest =
            new TableRequest().setStatement(createTableDDL)
            .setTableLimits(limits);
        /* this call will succeed or throw an exception */
        handle.doTableRequest(tableRequest,
                              60000, /* wait up to 60 sec */
                              1000); /* poll once per second */
        /* Create an index on the name field in the users table. */
        tableRequest = new TableRequest()
            .setStatement("CREATE INDEX IF NOT EXISTS nameIdx ON users(name)");
        handle.doTableRequest(tableRequest,
                              60000, /* wait up to 60 sec */
                              1000); /* poll once per second */

        /* ------ Snippet end -------*/

        /* Snippet start  "Add Data" */

        /* use the MapValue class and input the contents of a new row */
        MapValue value = new MapValue().put("id", 1).put("name", "myname");

        /* create the PutRequest, setting the required value and table name */
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName("users");

        /*
         * use the handle to execute the PUT request
         *
         * on success, PutResult.getVersion() returns a non-null value
         */
        PutResult putRes = handle.put(putRequest);
        if (putRes.getVersion() != null) {
            // success
        } else {
            // failure
        }

        /* ------ Snippet end -------*/

        /* Snippet start  "Adding JSON Data" */

        /* ------ Snippet end -------*/

        /* Snippet start  "" */
        /* ------ Snippet end -------*/

        /* Snippet start  "" */
        /* ------ Snippet end -------*/

    }

    @SuppressWarnings("unused")
    static void moreSnippets(NoSQLHandle handle) {
        /* Snippet start  "Adding JSON Data" */

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
        MapValue segments = new MapValue()
            .put("sports_lover", new TimestampValue("2018-11-30"))
            .put("book_reader", new TimestampValue("2018-12-01"));
        MapValue value = new MapValue()
            .put("cookie_id", 123) // fill in cookie_id field
            .put("ipaddr", "10.0.00.xxx")
            .put("audience_segment", segments);
        PutRequest putRequest = new PutRequest()
            .setValue(value)
            .setTableName("mytable");
        PutResult putRes = handle.put(putRequest);



        /* ------ Snippet end -------*/

        /* Snippet start  "Adding JSON Data part 2" */

        /* Construct a simple row in JSON */
        String jsonString = "{\"cookie_id\":123,\"ipaddr\":\"10.0.00.xxx\"," +
            "\"audience_segment\":{\"sports_lover\":\"2018-11-30\"," +
            "\"book_reader\":\"2018-12-01\"}}";

        putRequest = new PutRequest()
            .setValueFromJson(jsonString, null) // no options
            .setTableName("mytable");
        putRes = handle.put(putRequest);

        /* ------ Snippet end -------*/

        /* Snippet start  "Reading Data" */

        /* GET the row, first create the row key */
        MapValue key = new MapValue().put("id", 1);
        GetRequest getRequest = new GetRequest()
            .setKey(key)
            .setTableName("users");
        GetResult getRes = handle.get(getRequest);

        /* on success, GetResult.getValue() returns a non-null value */
        if (getRes.getValue() != null) {
            // success
        } else {
            // failure
        }

        /* ------ Snippet end -------*/

        /* Snippet start  "Using Queries" */

        /*
         * QUERY a table named "users", using the primary key field "name". The
         * table name is inferred from the query statement.
         */
        try (QueryRequest queryRequest = new QueryRequest()) {
            queryRequest
                .setStatement("SELECT * FROM users WHERE name = \"Taylor\"");

            /*
             * Queries can return partial results. It is necessary to loop,
             * reissuing the request until it is "done"
             */

            do {
                QueryResult queryResult = handle.query(queryRequest);

                /* process current set of results */
                List<MapValue> results = queryResult.getResults();
                for (MapValue qval : results) {
                    // handle result
                }
            }
            while (!queryRequest.isDone());
        }

        /* ------ Snippet end -------*/

        /* Snippet start  "Using Queries part 2 -- prepare" */

        /*
         * Perform the same query using a prepared statement. This is more
         * efficient if the query is executed repeatedly and required if
         * the query contains any bind variables.
         */
        String query = "DECLARE $name STRING; " +
            "SELECT * from users WHERE name = $name";

        PrepareRequest prepReq = new PrepareRequest().setStatement(query);

        /* prepare the statement */
        PrepareResult prepRes = handle.prepare(prepReq);

        /* set the bind variable and set the statement in the QueryRequest */
        prepRes.getPreparedStatement()
            .setVariable("$name", new StringValue("Taylor"));
        try (QueryRequest queryRequest = new QueryRequest()) {
            queryRequest.setPreparedStatement(prepRes);

            /* perform the query in a loop until done */
            do {
                QueryResult queryResult = handle.query(queryRequest);
                /* handle result */
            }
            while (!queryRequest.isDone());
        }

        /* ------ Snippet end -------*/

        /* Snippet start  "Delete Data" */

        /* identify the row to delete */
        MapValue delKey = new MapValue().put("id", 2);

        /* construct the DeleteRequest */
        DeleteRequest delRequest = new DeleteRequest()
            .setKey(key)
            .setTableName("users");

        /* Use the NoSQL handle to execute the delete request */
        DeleteResult del = handle.delete(delRequest);

        /* on success DeleteResult.getSuccess() returns true */
        if (del.getSuccess()) {
            // success, row was deleted
        } else {
            // failure, row either did not exist or conditional delete failed
        }

        /* ------ Snippet end -------*/

        /* Snippet start  "Modifying Tables" */

        /*
         * Alter the users table to modify table limits. When modifying
         * limits you cannot also modify the table schema or other table
         * state. These must be independent operations.
         */
        TableLimits limits = new TableLimits(250, 120, 10);
        TableRequest tableRequest = new TableRequest().setTableLimits(limits);
        /* this call will succeed or throw an exception */
        handle.doTableRequest(tableRequest,
                              60000, /* wait up to 60 sec */
                              1000); /* poll once per second */

        /* ------ Snippet end -------*/
    }

    static void yetMore(NoSQLHandle handle) {

        /* Snippet start  "Drop Tables and Indexes" */

        /* create the TableRequest to drop the users table */
        TableRequest tableRequest =
            new TableRequest().setStatement("drop table users");
        /* this call will succeed or throw an exception */
        handle.doTableRequest(tableRequest,
                              60000, /* wait up to 60 sec */
                              1000); /* poll once per second */

        /* ------ Snippet end -------*/

        /* Snippet start "Query iterable/iterator" */

        try (
            QueryRequest qreq = new QueryRequest()
                .setStatement("select * from MyTable");
            QueryIterableResult qir = handle.queryIterable(qreq)) {
             for( MapValue row : qir) {
                 // do something with row
             }
        }

        /* ------ Snippet end -------*/

        /* Snippet start "QueryIterableResult iterable/iterator" */

        try (QueryRequest qreq = new QueryRequest()
                 .setStatement("select * from foo") ) {

            for (MapValue row : handle.queryIterable(qreq)) {
              // do something with row
            }
        }

        /* ------ Snippet end -------*/
    }
}
