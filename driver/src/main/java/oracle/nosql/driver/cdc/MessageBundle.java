/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.cdc;

import java.util.List;
import java.time.Duration;

/**
 * One or more Messages returned from a call to Consumer.poll().
 */
public class MessageBundle {

    // Internal: the consumer that generated this bundle
    private Consumer consumer;

    // Internal: cursor data from the poll() that created this bundle
    private byte[] cursor;

    private long eventsRemaining;
    private List<Message> messages;

    /*
     * @hidden
     */
	public MessageBundle(List<Message> messages) {
        this.messages = messages;
	}

    /*
     * @hidden
     */
    public Consumer getConsumer() {
		return consumer;
	}

    /*
     * @hidden
     */
    public void setConsumer(Consumer consumer) {
        this.consumer = consumer;
    }

    /*
     * @hidden
     */
    public byte[] getCursor() {
		return cursor;
	}

    /*
     * @hidden
     */
    public void setCursor(byte[] cursor) {
		this.cursor = cursor;
	}

    /*
     * @hidden
     */
    public void setEventsRemaining(long remaining) {
		this.eventsRemaining = remaining;
	}

    /*
     * Return an estimate of the number of change events that are still remaining to
     * be consumed, not counting the events in this bundle. This can be used to monitor if a reader of
     * the events consumer is keeping up with change messages for the table.
     * This value applies to only the table data that this specific consumer can receive in poll() calls,
     * which may be less than the overall total if this consumer is one in a group of many active consumers.
     */
    public long getEventsRemaining() {
		return eventsRemaining;
	}

    /* Return the list of messages containing change event data. */
    public List<Message> getMessages() {
		return messages;
	} 

    /*
     * Mark the messages in the bundle as committed: all messages have been
     * fully read and consumed, and the messages should not be read again by any
     * current or future consumer in the group.
     *  
     * Note that this commit implies commits on all previously polled messages from the
     * same consumer (that is, messages that were returned from calls to poll() before
     * this one). Calling Commit() on a previous MessageBundle will have no effect.
     */
    public void commit(Duration timeout) {
        consumer.commit(timeout);
    }

    /*
     * Return true if the bundle is empty. This may happen if there was no
     * change data to read in the given timeframe of a poll().
     */
    public boolean isEmpty() {
        return (messages == null || messages.isEmpty());
    }
}

