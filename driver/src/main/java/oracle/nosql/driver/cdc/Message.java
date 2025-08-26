/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.cdc;

import java.util.List;

import oracle.nosql.driver.values.MapValue;

/**
 * A message from the change stream.
 * This message may contain multiple events. All events in the
 * message are for the same table.
 */
public class Message {
    private String tableName;
    private String compartmentOCID;
    private String tableOCID;
    private String version;
    private List<Event> events;

    Message(String tableName,
                  String compartmentOCID,
                  String tableOCID,
                  String version,
                  List<Event> events) {
        this.tableName = tableName;
        this.compartmentOCID = compartmentOCID;
        this.tableOCID = tableOCID;
        this.version = version;
        this.events = events;
    }

    /* Get the table name for this set of events. */
    public String getTableName() {
        return tableName;
    }

    /*
     * Get the compartment OCID for this set of events. If this is empty,
     * the compartment is assumed to be the default compartment
     * for the tenancy.
     */
    public String getCompartmentOCID() {
        return compartmentOCID;
    }

    /* Get the table OCID for this set of events. */
    public String getTableOCID() {
        return tableOCID;
    }

    /* get the version of the event format. */
    public String getVersion() {
        return version;
    }

    /* get the list of events. */
    public List<Event> getEvents() {
        return events;
    }
}
