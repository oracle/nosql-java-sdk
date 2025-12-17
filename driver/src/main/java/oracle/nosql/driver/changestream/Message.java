/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.changestream;

import java.util.List;

/**
 * A message from the change stream.
 * This message may contain multiple events. All events in the
 * message are for the same table.
 */
public class Message {
    private String tableName;
    private String compartmentOcid;
    private String tableOcid;
    private String version;
    private List<Event> events;

    /*
     * @hidden
     */
    public Message() {}

    /*
     * @hidden
     */
    public Message(String tableName,
                  String compartmentOcid,
                  String tableOcid,
                  String version,
                  List<Event> events) {
        this.tableName = tableName;
        this.compartmentOcid = compartmentOcid;
        this.tableOcid = tableOcid;
        this.version = version;
        this.events = events;
    }

    /* Get the table name for this set of events. */
    public String getTableName() {
        return tableName;
    }

    /*
     * Get the compartment Ocid for this set of events. If this is empty,
     * the compartment is assumed to be the default compartment
     * for the tenancy.
     */
    public String getCompartmentOcid() {
        return compartmentOcid;
    }

    /* Get the table Ocid for this set of events. */
    public String getTableOcid() {
        return tableOcid;
    }

    /* get the version of the event format. */
    public String getVersion() {
        return version;
    }

    /* get the list of events. */
    public List<Event> getEvents() {
        return events;
    }

    /*
     * @hidden
     */
    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    /*
     * @hidden
     */
    public void setCompartmentOcid(String ocid) {
       this.compartmentOcid = ocid;
    }

    /*
     * @hidden
     */
    public void setTableOcid(String ocid) {
        this.tableOcid = ocid;
    }

    /*
     * @hidden
     */
    public void setVersion(String version) {
        this.version = version;
    }

    /*
     * @hidden
     */
    public void setEvents(List<Event> events) {
        this.events = events;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Message {\n");
        sb.append(" tableName: { ").append(tableName).append(" }\n");
        sb.append(" compartmentOcid: { ").append(compartmentOcid).append(" }\n");
        sb.append(" tableOcid: { ").append(tableOcid).append(" }\n");
        sb.append(" version: { ").append(version).append(" }\n");
        sb.append(" events: { ").append(events).append(" }\n");
        sb.append("}");
        return sb.toString();
    }
}
