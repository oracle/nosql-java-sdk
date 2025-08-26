/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.cdc;

import java.time.Duration;

import oracle.nosql.driver.cdc.ConsumerRequest.RequestMode;
import oracle.nosql.driver.http.NoSQLHandleImpl;
import oracle.nosql.driver.NoSQLException;
import oracle.nosql.driver.NoSQLHandle;
import oracle.nosql.driver.OperationNotSupportedException;

/**
 * The main object used for Change Data Capture.
 *
 * NOTE: this class is not thread-safe, with the exception of calling
 * commit(), which can be done in other threads. Calling
 * poll() from multiple threads will result in undefined behavior.
 */
public class Consumer {
    private byte[] cursor;
    private ConsumerBuilder config;
    private NoSQLHandleImpl handle;

    Consumer(ConsumerBuilder builder) {
        builder.validate();
        this.handle = (NoSQLHandleImpl)builder.handle;

        ConsumerRequest req = new ConsumerRequest(RequestMode.CREATE).
                                     setBuilder(config);
        try {
            ConsumerResult res =
                (ConsumerResult) this.handle.getClient().execute(req);
            if (res.cursor == null) {
                throw new NoSQLException("Server returned invalid consumer cursor");
            }
            this.cursor = res.cursor;
        } catch (Exception e) {
            if (e.getMessage().contains("unknown opcode")) {
                throw new OperationNotSupportedException("CDC not supported by server");
            }
            throw e;
        }
    }

    /*
     * Mark the data from the most recent call to poll() as committed: the consumer has
     * completely processed the data and it should be considered "consumed".
     *
     * Note that this commit implies commits on all previously polled messages from the
     * same consumer (that is, messages that were returned from calls to poll() before
     * this one).
     *
     * This method is only necessary when using manual commit mode. Otherwise, in auto commit mode, the
     * commit is implied for all previous data every time poll() is called.
     */
    public void commit(Duration timeout) {
        commitInternal(this.cursor, timeout);
    }

    /*
     * Mark the data from the given MessageBundle as committed: the consumer has
     * completely processed the data and it should be considered "consumed".
     *
     * Note that this commit implies commits on all previously polled messages from the
     * same consumer (that is, messages that were returned from calls to poll() before
     * this one). Calling commitBundle() on a previous MessageBundle will have no effect.
     *
     * This method is only necessary when using manual commit mode. Otherwise, in auto commit mode, the
     * commit is implied for all previous data every time Consumer.poll() is called.
     */
    public void commitBundle(MessageBundle bundle, Duration timeout) {
        commitInternal(bundle.getCursor(), timeout);
    }

    /*
     * @hidden
     */
    public void commitInternal(byte[] cursor, Duration timeout) {
        /* TODO: use timeout */
        ConsumerRequest req = new ConsumerRequest(RequestMode.COMMIT).
                                     setCursor(cursor);
        try {
            ConsumerResult res =
                (ConsumerResult) handle.getClient().execute(req);
            // TODO: should commit update the cursor?
            if (res.cursor != null) {
                throw new NoSQLException("Consumer not committed on server side");
            }
        } catch (Exception e) {
            if (e.getMessage().contains("unknown opcode")) {
                throw new OperationNotSupportedException("CDC not supported by server");
            }
            throw e;
        }
    }

    /*
     * Close and release all resources for this consumer instance.
     *
     * Call this method if the application does not intend to continue using
     * this consumer. If this consumer was part of a group and has called poll(),
     * this call will trigger a rebalance such that data that was being directed
     * to this consumer will now be redistributed to other active consumers.
     *
     * If the consumer is in auto-commit mode, calling close() will implicitly call
     * commit() on the most recent events returned from poll().
     *
     * It is not required to call this method. If a consumer has not called poll()
     * within the maximum poll period, it will be considered closed by the system and a
     * rebalance may be triggered at that point.
     */
    public void close() {
        ConsumerRequest req = new ConsumerRequest(RequestMode.CLOSE).
                                     setCursor(cursor);
        try {
            ConsumerResult res =
                (ConsumerResult) handle.getClient().execute(req);
            if (res.cursor != null) {
                throw new NoSQLException("Consumer not closed on server side");
            }
        } catch (Exception e) {
            if (e.getMessage().contains("unknown opcode")) {
                throw new OperationNotSupportedException("CDC not supported by server");
            }
            throw e;
        }
    }


    /*
     * Get  Data Capture messages for a consumer.
     *
     * @param limit max number of change messages to return in the bundle. This value can be set to
     * zero to specify that this consumer is alive and active in the group without actually
     * returning any change events.
     *
     * @param waitTime max amount of time to wait for messages
     *
     * If this is the first call to poll() for a consumer, this call may trigger
     * a rebalance operation to redistribute change data across this and all other active consumers.
     * Note that the rebalance may not happen immediately; in the NoSQL system,
     * rebalanace operations are rate limitied to avoid excessive resource
     * usage when many consumers are being added to or removed from a group.
     *
     * This method in not thread-safe. Calling poll() on the same consumer instance
     * from multiple routines/threads will result in undefined behavior.
     */
    public MessageBundle poll(int limit, Duration waitTime) {
        /* TODO: config interval ? */
        long pollIntervalMs = 100;
        long waitMs = waitTime.toMillis();
        long startTime = System.currentTimeMillis();
        
        do {
            MessageBundle bundle = pollOnce(limit);
            if (!bundle.isEmpty()) {
                return bundle;
            }
            // if no messages, sleep for a short period and retry
            // if nearing end of waitTime, bail out
            long now = System.currentTimeMillis();
            if (((now - startTime) + pollIntervalMs) > waitMs) {
                return bundle;
            }
            try {
                Thread.sleep(pollIntervalMs);
            } catch (Exception e) {
                return bundle;
            }
        } while(true);
    }

    /*
     * @hidden
     */
    MessageBundle pollOnce(int limit) {
        PollRequest req = new PollRequest(cursor, limit);
        try {
            PollResult res =
                (PollResult) handle.getClient().execute(req);
            if (res.cursor == null) {
                throw new NoSQLException("Poll returned invalid cursor");
            }
            cursor = res.cursor;
            res.bundle.setConsumer(this);
            res.bundle.setCursor(res.cursor);
            return res.bundle;
        } catch (Exception e) {
            if (e.getMessage().contains("unknown opcode")) {
                throw new OperationNotSupportedException("CDC not supported by server");
            }
            throw e;
        }
    }
}
