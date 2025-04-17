/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

import oracle.nosql.driver.AuthorizationProvider;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.Region;
import oracle.nosql.driver.iam.SignatureProvider;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.values.MapValue;

public class HelloWorld {

    /* Name of your table */
    private static final String tableName = "HelloWorldTable";

    public static void main(String[] args) throws Exception {

        /* Set up an endpoint URL */
        Region region = getRegion(args);
        System.out.println("Using region: " + region);

        /*
         * Put your credentials in $HOME/.oci/config.
         */
        AuthorizationProvider ap = new SignatureProvider();

        /* Create a NoSQL handle to access the cloud service */
        NoSQLHandleConfig config = new NoSQLHandleConfig(region, ap);

        try (NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config))
        {
            /* Create a table and run operations. Be sure to close the handle */
            if (isDrop(args)) {
                dropTable(handle); // -drop was specified
            } else {
                helloWorld(handle);
            }
        }
    }

    /**
     * Create a table and do some operations.
     */
    private static void helloWorld(NoSQLHandle handle) throws Exception {

        /*
         * Create a simple table with an integer key and a single string data
         * field and set your desired table capacity.
         */
        String createTableDDL = "CREATE TABLE IF NOT EXISTS " + tableName +
                                "(id INTEGER, name STRING, " +
                                "PRIMARY KEY(id))";

        TableLimits limits = new TableLimits(1, 2, 1);
        TableRequest tableRequest =
            new TableRequest().setStatement(createTableDDL).
            setTableLimits(limits);
        System.out.println("Creating table " + tableName);
        /* this call will succeed or throw an exception */
        handle.doTableRequest(tableRequest,
                              60000, /* wait up to 60 sec */
                              1000); /* poll once per second */
        System.out.println("Table " + tableName + " is active");

        /* Make a row and write it */
        MapValue value = new MapValue().put("id", 29).put("name", "Tracy");
        PutRequest putRequest = new PutRequest().setValue(value)
            .setTableName(tableName);

        PutResult putResult = handle.put(putRequest);
        if (putResult.getVersion() != null) {
            System.out.println("Wrote " + value);
        } else {
            System.out.println("Put failed");
        }

        /* Make a key and read the row */
        MapValue key = new MapValue().put("id", 29);
        GetRequest getRequest = new GetRequest().setKey(key)
            .setTableName(tableName);

        GetResult getRes = handle.get(getRequest);
        System.out.println("Read " + getRes.getValue());

        /* At this point, you can see your table in the Identity Console */
    }

    /** Remove the table. */
    private static void dropTable(NoSQLHandle handle) throws Exception {

        /* Drop the table and wait for the table to move to dropped state */
        System.out.println("Dropping table " + tableName);
        TableRequest tableRequest = new TableRequest().setStatement
            ("DROP TABLE IF EXISTS " + tableName);
        /* this call will succeed or throw an exception */
        handle.doTableRequest(tableRequest,
                              60000, /* wait up to 60 sec */
                              1000); /* poll once per second */
        System.out.println("Table " + tableName + " has been dropped");
    }

    /** Get the end point from the arguments */
    private static Region getRegion(String[] args) {
        if (args.length > 0) {
            return Region.fromRegionId(args[0]);
        }

        System.err.println
            ("Usage: java -cp .:oracle-nosql-java-sdk-x.y.z/lib/* " +
             " HelloWorld <region> [-drop]\n");
        System.exit(1);
        return null;
    }

    /** Return true if -drop is specified */
    private static boolean isDrop(String[] args) {
        if (args.length < 2) {
            return false;
        }
        return args[1].equalsIgnoreCase("-drop");
    }
}
