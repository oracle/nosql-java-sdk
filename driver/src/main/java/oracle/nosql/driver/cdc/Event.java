/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.cdc;

import java.util.ArrayList;
import java.util.List;

public class Event {
    private List<Record> records;

    /*
     * @hidden
     */
    public Event(List<Record> records) {
        this.records = records;
    }

    /*
     * @hidden
     */
    public Event(Record record) {
        this.records = new ArrayList<Record>(1);
        this.records.add(record);
    }

    /*
     * Get the set of records in the change event. This will typically
     * be only one record, unless the stream has group-by-transaction
     * mode enabled, in which case there may be many records.
     */
    public List<Record> getRecords() {
        return records;
    }

    @Override
    public String toString() {
        return "{ Records: " + records + " }";
    }
}
