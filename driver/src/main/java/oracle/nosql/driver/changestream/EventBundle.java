/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.changestream;

import java.util.List;
import java.time.Duration;

import oracle.nosql.driver.values.MapValue;

/**
 * One or more Events returned from a call to Consumer.poll().
 */
public class EventBundle {

    // Internal: the consumer that generated this bundle
    private Consumer consumer;

    // Internal: cursor data from the poll() that created this bundle
    private byte[] cursor;

    private List<Event> events;

    /* internal use */
    private MapValue metadata;

    /*
     * @hidden
     */
    public EventBundle(List<Event> events) {
        this.events = events;
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
    public MapValue getMetadata() {
        return metadata;
    }

    /*
     * @hidden
     */
    public void setMetadata(MapValue metadata) {
        this.metadata = metadata;
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

    /* Return the list of events containing change event data. */
    public List<Event> getEvents() {
        return events;
    }

    /*
     * Mark the events in the bundle as committed: all events have been
     * fully read and consumed, and the events should not be read again by any
     * current or future consumer in the group.
     *
     * Note that this commit implies commits on all previously polled events from the
     * same consumer (that is, events that were returned from calls to poll() before
     * this one). Calling Commit() on a previous EventBundle will have no effect.
     */
    public void commit(Duration timeout) {
        consumer.commitBundle(this, timeout);
    }

    /*
     * Return true if the bundle is empty. This may happen if there was no
     * change data to read in the given timeframe of a poll().
     */
    public boolean isEmpty() {
        return (events == null || events.isEmpty());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("EventBundle {\n");
        sb.append(" consumer: { ").append(consumer).append(" }\n");
        if (cursor == null) {
            sb.append(" cursor: { (null) }\n");
        } else {
            sb.append(" cursor: { (size=").append(cursor.length).append(") }\n");
        }
        sb.append(" events: { ").append(events).append(" }\n");
        sb.append("}");
        return sb.toString();
    }
}

