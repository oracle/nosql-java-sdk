/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.cdc;

import oracle.nosql.driver.NoSQLHandle;

public class CDC {
}

\u001a

type ConsumerTableMetrics struct {
    // The table name for this table
    TableName string

    // The compartment OCID for this table.
    CompartmentOCID string

    // The table OCID for this table.
    TableOCID string

    // The number of messages remaining to be consumed. This may be an
    // estimate, and is based on the number of messages between the last
    // committed message and the most recent message produced.
    RemainingMessages uint64

    // The number of bytes remaining to be consumed. This may be an
    // estimate, and is based on the number of bytes between the last
    // committed message and the most recent message produced.
    RemainingBytes uint64

    // The timestamp of the oldest uncommitted message. This maybe an
    // estimate, and it represents the time that the message was placed
    // into the stream.
    OldestMessageTimestamp time.Time
}

// ConsumerMetrics encapsulates metrics for all tables in a consumer group.
type ConsumerMetrics struct {
    TableMetrics []ConsumerTableMetrics
}

// Get the metrics for this specific consumer. The metrics data returned is
// relevant only to the change data that this consumer can read (as opposed
// to the metrics for the group as a whole).
func (cc *Consumer) GetMetrics() (*ConsumerMetrics, error) {
    // TODO
    return nil, fmt.Errorf("function not implemented yet")
}

// Get metrics for the specified consumer group. The metrics are for the group
// as a whole (as opposed to metrics for each individual consumer in a group).
func (c *Client) GetConsumerMetrics(groupID string) (*ConsumerMetrics, error) {
    // TODO
    return nil, fmt.Errorf("function not implemented yet")
}


// Enable  Data Capture on an existing table.
//
// This is a convenience method to simplify enabling CDC. It creates and
// executes a TableRequest with CDCEnabled set to true.
func (c *Client) EnableDataCapture(tableName string, compartmentOCID string) error {
    // TODO: resolve compartmentOCID: this is currently on a per-handle-only basis
    tableReq := &TableRequest{
        TableName: tableName,
        CDCConfig: &TableCDCConfig{Enabled: true},
    }
    _, err := c.DoTableRequest(tableReq)
    // TODO: is this an async operation? Should it wait for completion?
    return err
}

// Create a single-table consumer with all default parameters.
//
// This is a convenience method for crating a consumer for a single table.
// It is equivalent to:
//
//    config := c.CreateConsumerConfig().
//        AddTable(tableName, "", FirstUncommitted, nil).
//        GroupID(groupID)
//    consumer, err := c.CreateConsumer(config)
func (c *Client) CreateSimpleConsumer(tableName, groupID string) (*Consumer, error) {
    config := c.CreateConsumerConfig().
        AddTable(tableName, "", FirstUncommitted, nil).
        GroupID(groupID)
    return c.CreateConsumer(config)
}

// Get  Data Capture messages for a consumer.
//
// limit: max number of change messages to return in the bundle. This value can be set to
// zero to specify that this consumer is alive and active in the group without actually
// returning any change events.
//
// waitTime: max amount of time to wait for messages
//
// If this is the first call to poll() for a consumer, this call may trigger
// a rebalance operation to redistribute change data across this and all other active consumers.
// Note that the rebalance may not happen immediately; in the NoSQL system,
// rebalanace operations are rate limitied to avoid excessive resource
// usage when many consumers are being added to or removed from a group.
//
// This method in not thread-safe. Calling poll() on the same consumer instance
// from multiple routines/threads will result in undefined behavior.
func (cc *Consumer) poll(limit int, waitTime time.Duration) (bundle *MessageBundle, err error) {
    pollInterval := 100 * time.Millisecond // TODO: config?
    start := time.Now();
    for {
        bundle, err = cc.pollOnce(limit)
        if err != nil {
            return
        }
        if len(bundle.Messages) > 0 {
            return
        }
        // if no messages, sleep for a short period and retry
        // TODO: backoff algorithm?
        // if nearing end of waitTime, bail out
        if time.Since(start) + pollInterval > waitTime {
            return
        }

        time.Sleep(pollInterval)
    }
}

func (cc *Consumer) pollOnce(limit int) (*MessageBundle, error) {
    req := &cdcpollRequest{consumer: cc, maxEvents: limit}
    res, err := cc.client.execute(req)
    if err != nil {
        if strings.Contains(err.Error(), "unknown opcode") {
            return nil, nosqlerr.New(nosqlerr.OperationNotSupported, "CDC not supported by server")
        }
        return nil, err
    }
    if res, ok := res.(*cdcpollResult); ok {
        cc.cursor = res.cursor
        res.bundle.consumer = cc
        res.bundle.cursor = res.cursor
        return res.bundle, nil
    }
    return nil, errUnexpectedResult
}

// Mark the data from the most recent call to [Consumer.poll] as committed: the consumer has
// completely processed the data and it should be considered "consumed".
//
// Note that this commit implies commits on all previously polled messages from the
// same consumer (that is, messages that were returned from calls to poll() before
// this one).
//
// This method is only necessary when using manual commit mode. Otherwise, in auto commit mode, the
// commit is implied for all previous data every time [Consumer.poll] is called.
func (cc *Consumer) Commit(timeout time.Duration) error {
    return cc.client.commitInternal(cc.cursor, timeout)
}

func (c *Client) commitInternal(cursor []byte, timeout time.Duration) error {
    // TODO: use timeout
    req := &cdcConsumerRequest{cursor: cursor, mode: CommitConsumer}
    res, err := c.execute(req)
    if err != nil {
        if strings.Contains(err.Error(), "unknown opcode") {
            return nosqlerr.New(nosqlerr.OperationNotSupported, "CDC not supported by server")
        }
        return err
    }
    if res, ok := res.(*cdcConsumerResult); ok {
        // TODO: should commit update the cursor?
        if res.cursor != nil {
            return nosqlerr.New(nosqlerr.UnknownError, "Consumer not committed on server side")
        }
        return nil
    }
    return errUnexpectedResult
}

// Mark the data from the given MessageBundle as committed: the consumer has
// completely processed the data and it should be considered "consumed".
//
// Note that this commit implies commits on all previously polled messages from the
// same consumer (that is, messages that were returned from calls to poll() before
// this one). Calling CommitBundle() on a previous MessageBundle will have no effect.
//
// This method is only necessary when using manual commit mode. Otherwise, in auto commit mode, the
// commit is implied for all previous data every time [Consumer.poll] is called.
func (cc *Consumer) CommitBundle(bundle *MessageBundle, timeout time.Duration) error {
    return cc.client.commitInternal(bundle.cursor, timeout)
}

// AddTable adds a table to an existing consumer. The table must have already been
// CDC enabled via the OCI console or a NoSQL SDK [Client.DoTableRequest] call.
// If the given table already exists in the group, this call is ignored and will return no error.
//
// Note this will affect all active consumers using the same group ID.
//
// tableName: required. This may be the OCID of the table, if available.
//
// compartmentOCID: This is optional. If empty, the default compartment OCID
// for the tenancy is used.
//
// startLocation: Specify the position of the first element to read in the change stream.
//
// startTime: If start location specifies AtTime, the startTime field is required to be non-nil.
func (cc *Consumer) AddTable(tableName string, compartmentOCID string, startLocation LocationType, startTime *time.Time) error {
    // get existing config
    if cc.config == nil {
        return fmt.Errorf("can't add table to consumer: missing internal config")
    }
    if cc.cursor == nil {
        return fmt.Errorf("can't add table to consumer: missing cursor information")
    }
    // check if table already in config
    if cc.config.tableIndex(tableName, compartmentOCID) >= 0 {
        return nil
    }
    // add table to config
    cc.config.AddTable(tableName, compartmentOCID, startLocation, startTime)
    err := cc.client.validateTableConfig(cc.config)
    if err != nil {
        return err
    }

    // call method to update config on cursor
    req := &cdcConsumerRequest{config: cc.config, cursor: cc.cursor, mode: UpdateConsumer}
    res, err := cc.client.execute(req)
    if err != nil {
        if strings.Contains(err.Error(), "unknown opcode") {
            return nosqlerr.New(nosqlerr.OperationNotSupported, "CDC not supported by server")
        }
        return err
    }
    if resp, ok := res.(*cdcConsumerResult); ok {
        if resp.cursor == nil {
            return nosqlerr.New(nosqlerr.BadProtocolMessage, "Response missing cursor")
        }
        cc.cursor = resp.cursor
        return nil
    }
    return errUnexpectedResult
}

func (cc *ConsumerConfig) tableIndex(tableName, compartmentOCID string) int {
    for x, elem := range cc.Tables {
        if strings.EqualFold(elem.tableName, tableName) ||
            strings.EqualFold(elem.tableOCID, tableName) {
            if strings.EqualFold(elem.compartmentOCID, compartmentOCID) {
                return x
            }
        }
    }
    return -1
}

// RemoveTable removes a table from an existing change consumer group.
// If the given table does not exist in the group, this call is ignored and
// will return no error.
//
// Note this will affect all active consumers using the same group ID.
//
// tablename: required.
//
// compartmentOCID: This is optional. If empty, the default compartment OCID
// for the tenancy is used.
func (cc *Consumer) RemoveTable(tableName string, compartmentOCID string) error {
    // get existing config
    if cc.config == nil {
        return fmt.Errorf("can't remove table from consumer: missing internal config")
    }
    if cc.cursor == nil {
        return fmt.Errorf("can't remove table from consumer: missing cursor information")
    }

    // remove table from config
    index := cc.config.tableIndex(tableName, compartmentOCID)
    if index < 0 {
        return nil
    }
    numTables := len(cc.config.Tables)
    if numTables == 1 {
        return fmt.Errorf("can't remove last table from consumer: use DeleteConsumerGroup() instead")
    }
    n := index
    for ; n < (numTables - 1) ; n++ {
        cc.config.Tables[n] = cc.config.Tables[n+1]
    }
    cc.config.Tables = cc.config.Tables[:n]

    err := cc.client.validateTableConfig(cc.config)
    if err != nil {
        return err
    }

    // call method to update config on cursor
    req := &cdcConsumerRequest{config: cc.config, cursor: cc.cursor, mode: UpdateConsumer}
    res, err := cc.client.execute(req)
    if err != nil {
        if strings.Contains(err.Error(), "unknown opcode") {
            return nosqlerr.New(nosqlerr.OperationNotSupported, "CDC not supported by server")
        }
        return err
    }
    if resp, ok := res.(*cdcConsumerResult); ok {
        if resp.cursor == nil {
            return nosqlerr.New(nosqlerr.BadProtocolMessage, "Response missing cursor")
        }
        cc.cursor = resp.cursor
        return nil
    }
    return errUnexpectedResult
}

// Close and release all resources for this consumer instance.
//
// Call this method if the application does not intend to continue using
// this consumer. If this consumer was part of a group and has called poll(),
// this call will trigger a rebalance such that data that was being directed
// to this consumer will now be redistributed to other active consumers.
//
// If the consumer is in auto-commit mode, calling Close() will implicitly call
// Commit() on the most recent events returned from poll().
//
// It is not required to call this method. If a consumer has not called [Consumer.poll]
// within the maximum poll period, it will be considered closed by the system and a
// rebalance may be triggered at that point.
func (cc *Consumer) Close() error {
    req := &cdcConsumerRequest{cursor: cc.cursor, mode: CloseConsumer}
    res, err := cc.client.execute(req)
    if err != nil {
        if strings.Contains(err.Error(), "unknown opcode") {
            return nosqlerr.New(nosqlerr.OperationNotSupported, "CDC not supported by server")
        }
        return err
    }
    if res, ok := res.(*cdcConsumerResult); ok {
        if res.cursor != nil {
            return nosqlerr.New(nosqlerr.UnknownError, "Consumer not closed on server side")
        }
        return nil
    }
    return errUnexpectedResult
}

// Get the minimum number of consumers needed in order to process all change
// data given a set of tables and start location, and a desired amount of time to process
// the data.
//
// config: use this field to specify the tables and their start locations.
// all other data in the config is ignored, with the exception of GroupID (see below).
//
// timeToProcess: specify the desired time to finish processing of the change
// data in the given tables from their respective start points.
//
// This function returns an estimate based only on the amount of data that exists
// in the change streams for the given tables, and the fastest rate that the system can
// deliver that data to each consumer. It does not take into account any additional
// processing time used by the consumer applications.
//
// Note: if GroupID is specified in the config, and the start location in the config is
// NextUncommitted, and this is an existing group, this call will return the count based
// on the remaining data that has yet to be read and committed for the existing group.
func (c *Client) GetMinimumConsumerCount(config *ConsumerConfig, processTime time.Duration) (int, error) {
    // TODO
    return 0, fmt.Errorf("function not implemented yet")
}

// AddTableToConsumerGroup adds a table to an existing change consumer group
// based on the groupID. The table must have already been
// CDC enabled via the OCI console or a NoSQL SDK TableRequest call.
// If the given group ID does not exist in the system, this call will return an error.
// If the given table already exists in the group, this call is ignored and
// will return no error.
//
// groupID: required.
//
// tablename: required.
//
// compartmentOCID: This is optional. If empty, the default compartment OCID
// for the tenancy is used.
//
// startLocation: Specify the position of the first element to read in the change stream.
//
// startTime: If start location specifies AtTime, the startTime field is required to be non-nil.
func (c *Client) AddTableToConsumerGroup(groupID string, tableName string, compartmentOCID string, startLocation LocationType, startTime *time.Time) error {
    // TODO
    return fmt.Errorf("function not implemented yet")
}

// RemoveTableFromConsumerGroup removes a table from an existing change consumer group
// based on the groupID.
// If the given group ID does not exist in the system, this call will return an error.
// If the given table does not exist in the group, this call is ignored and
// will return no error.
//
// groupID: required.
//
// tablename: required.
//
// compartmentOCID: This is optional. If empty, the default compartment OCID
// for the tenancy is used.
func (c *Client) RemoveTableFromConsumerGroup(groupID string, tableName string, compartmentOCID string) error {
    // TODO
    return fmt.Errorf("function not implemented yet")
}

// DeleteConsumerGroup deletes all metadata (current poll positions,
// committed message locations, etc) from a consumer group, effectively
// removing it from the system.
//
// Any active consumers currently polling this group will get errors on
// all successive calls to poll() after this call completes.
//
// compartmentOCID: This is optional. If empty, the default compartment OCID
// for the tenancy is used.
//
//    Note: This method is dangerous, as it will affect any currently
//          running consumers for this group.
//
// This method is typically used during testing, where it may be desirable
// to immediately stop and clean up all resources used by a group.
func (c *Client) DeleteConsumerGroup(groupID string, compartmentOCID string) error {
    cfg := &ConsumerConfig{GroupId: groupID, CompartmentOCID: compartmentOCID}
    req := &cdcConsumerRequest{config: cfg, mode: DeleteConsumer}
    res, err := c.execute(req)
    if err != nil {
        if strings.Contains(err.Error(), "unknown opcode") {
            return nosqlerr.New(nosqlerr.OperationNotSupported, "CDC not supported by server")
        }
        return err
    }
    if res, ok := res.(*cdcConsumerResult); ok {
        // delete should never return a cursor
        if res.cursor != nil {
            return nosqlerr.New(nosqlerr.UnknownError, "Consumer not deleted on server side")
        }
        return nil
    }
    return errUnexpectedResult
}

func (c *Client) simpleCDCTest() error {
    // Create a single (non-grouped) consumer for a single table.
    consumer, err := c.CreateSimpleConsumer("test_table", "group1")
    if err != nil {
        return fmt.Errorf("error creating change consumer: %v", err)
    }

    // read data from the stream, returning only if the stream has ended.
    for {
        // wait up to one second to read up to 10 events
        message, err := consumer.poll(10, time.Duration(1*time.Second))
        if err != nil {
            return fmt.Errorf("error getting CDC messages: %v", err)
        }
        // If the time elapsed but there were no messages to read, the returned message
        // will have an empty array of events.
        fmt.Printf("Received message: %v", message)
    }
}

func (c *Client) multiTableCDCTest() error {

    // Create a new consumer for two tables, starting with the most current entry in
    // each table's CDC stream.
    config := c.CreateConsumerConfig().
        AddTable("client_info", "", Latest, nil).
        AddTable("location_data", "", Latest, nil).
        GroupID("test_group").
        CommitAutomatic()
    consumer, err := c.CreateConsumer(config)
    if err != nil {
        return err
    }

    // Read 10 messages from the CDC stream.
    for i := 0; i < 10; i++ {
        // wait up to one second to read up to 10 events
        message, err := consumer.poll(10, time.Duration(1*time.Second))
        if err != nil {
            return fmt.Errorf("error getting CDC messages: %v", err)
        }
        // If the time elapsed but there were no messages to read, the returned message
        // will have an empty array of events.
        fmt.Printf("Received message: %v", message)
    }

    return nil
}
