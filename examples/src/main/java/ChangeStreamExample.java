/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

import java.time.Duration;

import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.NoSQLHandleConfig;
import oracle.nosql.driver.NoSQLHandleFactory;
import oracle.nosql.driver.changestream.Consumer;
import oracle.nosql.driver.changestream.ConsumerBuilder;
import oracle.nosql.driver.changestream.Event;
import oracle.nosql.driver.changestream.EventBundle;
import oracle.nosql.driver.changestream.Record;
import oracle.nosql.driver.changestream.StartLocation;
import oracle.nosql.driver.ops.DeleteRequest;
import oracle.nosql.driver.ops.PutRequest;
import oracle.nosql.driver.ops.TableLimits;
import oracle.nosql.driver.ops.TableRequest;
import oracle.nosql.driver.values.MapValue;

/**
 * Enable change streaming, generate changes, and process them for one minute.
 * Requires a cloud service endpoint with change streaming support and
 * permissions to create tables and use change streams.
 *
 * To run:
 *   java -cp .:../lib/nosqldriver.jar ChangeStreamExample \
 *      <region> [-configFile <path>]
 * See Common.java for authentication setup.
 *
 * The table and consumer group are retained after the example finishes.
 * Reusing the group ID resumes from its committed position. A new group
 * starts at the earliest available change, including changes from prior runs.
 */
public class ChangeStreamExample {

    private static final String TABLE_NAME = "examplesChangeStream";
    private static final String GROUP_ID = "examplesChangeStreamGroup";

    public static void main(String[] args) throws Exception {
        Common setup = new Common("ChangeStreamExample");
        setup.validate(args);

        NoSQLHandleConfig config = new NoSQLHandleConfig(setup.getEndpoint());
        config.setAuthorizationProvider(setup.getAuthProvider());

        try (NoSQLHandle handle = NoSQLHandleFactory.createNoSQLHandle(config))
        {
            TableRequest request = new TableRequest()
                .setStatement("CREATE TABLE IF NOT EXISTS " + TABLE_NAME +
                              "(id INTEGER, name STRING, PRIMARY KEY(id))")
                .setTableLimits(new TableLimits(50, 50, 1));
            handle.doTableRequest(request, 60000, 1000);

            /* Wait for streaming to be enabled before generating changes. */
            handle.enableChangeStreaming(TABLE_NAME, null, true, 60000, 1000);

            Consumer consumer = new ConsumerBuilder()
                .handle(handle)
                .addTable(TABLE_NAME, null, StartLocation.earliest())
                .groupId(GROUP_ID)
                .commitManual()
                .build();
            try {
                /* Insert, update, and delete a row to generate changes. */
                handle.put(new PutRequest().setTableName(TABLE_NAME)
                           .setValue(new MapValue().put("id", 1)
                                     .put("name", "Tracy")));
                handle.put(new PutRequest().setTableName(TABLE_NAME)
                           .setValue(new MapValue().put("id", 1)
                                     .put("name", "Tracy Smith")));
                handle.delete(new DeleteRequest().setTableName(TABLE_NAME)
                              .setKey(new MapValue().put("id", 1)));

                System.out.println("Polling changes for one minute...");
                long deadline = System.nanoTime() +
                    Duration.ofMinutes(1).toNanos();
                while (System.nanoTime() < deadline) {
                    EventBundle bundle = consumer.poll(100,
                                                       Duration.ofSeconds(5));
                    /* An empty poll does not mean the stream has ended. */
                    if (bundle.isEmpty()) {
                        continue;
                    }
                    for (Event event : bundle.getEvents()) {
                        /* An event can contain multiple records. */
                        for (Record record : event.getRecords()) {
                            process(record);
                        }
                    }
                    /* Commit only after every record was processed successfully.
                     * Processing should tolerate replay if a failure occurs
                     * before the commit completes.
                     */
                    consumer.commit(Duration.ofSeconds(10));
                }
            } finally {
                /* Consumer must be closed before its NoSQL handle. */
                consumer.close();
            }
        }
    }

    private static void process(Record record) {
        /* Key fields are separate from the row images. */
        if (record.getCurrentImage() == null) {
            System.out.println("DELETE key=" + record.getRecordKey());
        } else {
            System.out.println("PUT key=" + record.getRecordKey() +
                               " value=" + record.getCurrentImage().getValue());
        }
        if (record.getBeforeImage() != null) {
            System.out.println("  previous value=" +
                               record.getBeforeImage().getValue());
        }
    }
}
