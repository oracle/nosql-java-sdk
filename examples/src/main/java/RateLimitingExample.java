/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.ReadThrottlingException;
import oracle.nosql.driver.WriteThrottlingException;
import oracle.nosql.driver.ops.GetRequest;
import oracle.nosql.driver.ops.GetResult;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.PutResult;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.values.MapValue;

/**
 * A simple program to demonstrate how to enable and use rate limiting.
 *
 * This example should only be run against CloudSim, as the on-premise
 * Oracle NoSQL database currently does not report read/write throughput
 * used by rate limiting logic.
 *
 * This example could be used with the cloud service, but it generates a
 * significant amount of data, which may use up your resources.
 *
 * To run:
 *   java -cp .:../lib/nosqldriver.jar RateLimitingExample \
 *      <endpoint_or_region> [-configFile path_to_iam_config]
 *
 * The endpoint and arguments vary with the environment. For details see
 * running instructions in Common.java
 */
public class RateLimitingExample {

    public static void main(String[] args) throws Exception {

        /* Validate arguments and get an authorization provider */
        Common setup = new Common("RateLimitingExample");
        setup.validate(args);

        /* Set up the handle configuration */
        NoSQLHandleConfig config = new NoSQLHandleConfig(setup.getEndpoint());
        config.setAuthorizationProvider(setup.getAuthProvider());

        /* enable rate limiting */
        config.setRateLimitingEnabled(true);

        /*
         * Note: the amount of table limits used by this client can be
         * configured using config.setDefaultRateLimitingPercentage().
         */

        /*
         * Open the handle in a try-with-resources statement to ensure
         * proper closing of resources.
         */
        try (NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config))
        {
            /*
             * Create a simple table with an integer key and a single
             * string field. Set the table limits to 50 RUs/WUs per
             * second.
             */
            String tableName = "audienceData";
            final String createTableStatement =
                "CREATE TABLE IF NOT EXISTS " + tableName +
                "(id LONG, data STRING, PRIMARY KEY(id))";

            TableRequest tableRequest = new TableRequest()
                .setStatement(createTableStatement)
                .setTableLimits(new TableLimits(50, 50, 50));
            System.out.println("Creating table " + tableName);
            /* this call will succeed or throw an exception */
            handle.doTableRequest(tableRequest,
                                  60000, /* wait up to 60 sec */
                                  1000); /* poll once per second */
            System.out.println("Created table " + tableName);

            /* create records of random sizes */
            int minSize = 100;
            int maxSize = 10000;

            /*
             * Do a bunch of write ops, verify our usage matches limits
             */
            doRateLimitedOps(handle,
                10 /* seconds */,
                true, /* writes */
                50, /* WUs limit */
                2000, /* maxRows */
                tableName,
                minSize,
                maxSize);

            /*
             * Do a bunch of read ops, verify our usage matches limits
             */
            doRateLimitedOps(handle,
                10 /* seconds */,
                false, /* reads */
                50, /* RUs limit */
                2000, /* maxRows */
                tableName,
                minSize,
                maxSize);

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

    /**
     * Runs puts and gets continuously for N seconds.
     *
     * Verify that the resultant RUs/WUs used match the
     * given rate limits.
     */
    private static void doRateLimitedOps(NoSQLHandle handle, int numSeconds,
        boolean doWrites, int limit, int maxRows,
        String tableName, int minSize, int maxSize) {

        PutRequest putRequest = new PutRequest().setTableName(tableName);
        GetRequest getRequest = new GetRequest().setTableName(tableName);

        MapValue key = new MapValue();
        MapValue value = new MapValue();

        /* generate a stringBuilder of maxSize with all "x"s in it */
        StringBuilder userData = null;
        if (doWrites) {
            userData = new StringBuilder();
            for (int x=0; x<maxSize; x++) {
                userData.append("x");
            }
        }

        long startTime = System.currentTimeMillis();
        long endTime = startTime + (numSeconds * 1000);

        System.out.println("Running continuous " +
            ((doWrites)?"writes":"reads") + " for " + numSeconds + " seconds");

        /* keep track of how many units we used */
        int unitsUsed = 0;

        /*
         * with rate limiting enabled, we can find the amount of time our
         * operation was delayed due to rate limiting by getting the value
         * from the result using getRateLimitDelayedMs().
         */
        int delayMs = 0;

        do {
            int id = (int)(Math.random() * (double)maxRows);
            try {
                if (doWrites) {
                    value.put("id", id);
                    int recSize =
                        (int)(Math.random() * (double)(maxSize - minSize));
                    recSize += minSize;
                    value.put("data", userData.substring(0, recSize));
                    putRequest.setValue(value);
                    PutResult pres = handle.put(putRequest);
                    unitsUsed += pres.getWriteUnits();
                    delayMs += pres.getRateLimitDelayedMs();
                } else {
                    key.put("id", id);
                    getRequest.setKey(key);
                    GetResult gres = handle.get(getRequest);
                    unitsUsed += gres.getReadUnits();
                    delayMs += gres.getRateLimitDelayedMs();
                }
            /* we should not get throttling exceptions */
            } catch (WriteThrottlingException wte) {
                System.err.println("Got unexpected write throttling exception");
                throw wte;
            } catch (ReadThrottlingException rte) {
                System.err.println("Got unexpected read throttling exception");
                throw rte;
            }
        } while (System.currentTimeMillis() < endTime);

        numSeconds = (int)((System.currentTimeMillis() - startTime) / 1000);

        unitsUsed = unitsUsed / numSeconds;

        if (unitsUsed < (int)((double)limit * 0.8) ||
            unitsUsed > (int)((double)limit * 1.2)) {
            final String msg;
            if (doWrites) {
                msg = "Writes: expected around " + limit +
                    " WUs, got " + unitsUsed;
            } else {
                msg = "Reads: expected around " + limit +
                    " RUs, got " + unitsUsed;
            }
            throw new RuntimeException(msg);
        }

        System.out.println(((doWrites)?"Writes":"Reads") +
            ": average usage = " + unitsUsed +
            ((doWrites)?"WUs":"RUs") + " (expected around " + limit + ")");

        System.out.println(
            "Total rate limiter delay time = " + delayMs + "ms");
    }

}
