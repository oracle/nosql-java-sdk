/*-
 * Copyright (c) 2011, 2021 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import java.io.IOException;

/**
 * Defines the durability characteristics associated with a standalone write
 * (put or update) operation.
 * <p>
 * This is currently only supported in On-Prem installations. It is ignored
 * in the cloud service.
 * <p>
 * The overall durability is a function of the {@link SyncPolicy} and {@link
 * ReplicaAckPolicy} in effect for the Master, and the {@link SyncPolicy} in
 * effect for each Replica.
 * </p>
 */
public class Durability {

    /**
     * A convenience constant that defines a durability policy with COMMIT_SYNC
     * for Master commit synchronization.
     *
     * The policies default to COMMIT_NO_SYNC for commits of replicated
     * transactions that need acknowledgment and SIMPLE_MAJORITY for the
     * acknowledgment policy.
     */
    public static final Durability COMMIT_SYNC =
        new Durability(SyncPolicy.SYNC,
                       SyncPolicy.NO_SYNC,
                       ReplicaAckPolicy.SIMPLE_MAJORITY);

    /**
     * A convenience constant that defines a durability policy with
     * COMMIT_NO_SYNC for Master commit synchronization.
     *
     * The policies default to COMMIT_NO_SYNC for commits of replicated
     * transactions that need acknowledgment and SIMPLE_MAJORITY for the
     * acknowledgment policy.
     */
    public static final Durability COMMIT_NO_SYNC =
        new Durability(SyncPolicy.NO_SYNC,
                       SyncPolicy.NO_SYNC,
                       ReplicaAckPolicy.SIMPLE_MAJORITY);

    /**
     * A convenience constant that defines a durability policy with
     * COMMIT_WRITE_NO_SYNC for Master commit synchronization.
     *
     * The policies default to COMMIT_NO_SYNC for commits of replicated
     * transactions that need acknowledgment and SIMPLE_MAJORITY for the
     * acknowledgment policy.
     */
    public static final Durability COMMIT_WRITE_NO_SYNC =
        new Durability(SyncPolicy.WRITE_NO_SYNC,
                       SyncPolicy.NO_SYNC,
                       ReplicaAckPolicy.SIMPLE_MAJORITY);

    /**
     * Defines the synchronization policy to be used when committing a
     * transaction. High levels of synchronization offer a greater guarantee
     * that the transaction is persistent to disk, but trade that off for
     * lower performance.
     */
    public enum SyncPolicy {

        /*
         * WARNING: To avoid breaking serialization compatibility, the order of
         * the values must not be changed and new values must be added at the
         * end.
         */

        /**
         *  Write and synchronously flush the log on transaction commit.
         *  Transactions exhibit all the ACID (atomicity, consistency,
         *  isolation, and durability) properties.
         */
        SYNC,

        /**
         * Do not write or synchronously flush the log on transaction commit.
         * Transactions exhibit the ACI (atomicity, consistency, and isolation)
         * properties, but not D (durability); that is, database integrity will
         * be maintained, but if the application or system fails, it is
         * possible some number of the most recently committed transactions may
         * be undone during recovery. The number of transactions at risk is
         * governed by how many log updates can fit into the log buffer, how
         * often the operating system flushes dirty buffers to disk, and how
         * often log checkpoints occur.
         */
        NO_SYNC,

        /**
         * Write but do not synchronously flush the log on transaction commit.
         * Transactions exhibit the ACI (atomicity, consistency, and isolation)
         * properties, but not D (durability); that is, database integrity will
         * be maintained, but if the operating system fails, it is possible
         * some number of the most recently committed transactions may be
         * undone during recovery. The number of transactions at risk is
         * governed by how often the operating system flushes dirty buffers to
         * disk, and how often log checkpoints occur.
         */
        WRITE_NO_SYNC;
    }

    /**
     * A replicated environment makes it possible to increase an application's
     * transaction commit guarantees by committing changes to its replicas on
     * the network. ReplicaAckPolicy defines the policy for how such network
     * commits are handled.
     */
    public enum ReplicaAckPolicy {

        /**
         * All replicas must acknowledge that they have committed the
         * transaction. This policy should be selected only if your replication
         * group has a small number of replicas, and those replicas are on
         * extremely reliable networks and servers.
         */
        ALL,

        /**
         * No transaction commit acknowledgments are required and the master
         * will never wait for replica acknowledgments. In this case,
         * transaction durability is determined entirely by the type of commit
         * that is being performed on the master.
         */
        NONE,

        /**
         * A simple majority of replicas must acknowledge that they have
         * committed the transaction. This acknowledgment policy, in
         * conjunction with an election policy which requires at least a simple
         * majority, ensures that the changes made by the transaction remains
         * durable if a new election is held.
         */
        SIMPLE_MAJORITY;
    }

    /* The sync policy in effect on the Master node. */
    private final SyncPolicy masterSync;

    /* The sync policy in effect on a replica. */
    final private SyncPolicy replicaSync;

    /* The replica acknowledgment policy to be used. */
    final private ReplicaAckPolicy replicaAck;

    /**
     * Creates an instance of a Durability specification.
     *
     * @param masterSync the SyncPolicy to be used when committing the
     * transaction on the Master.
     * @param replicaSync the SyncPolicy to be used remotely, as part of a
     * transaction acknowledgment, at a Replica node.
     * @param replicaAck the acknowledgment policy used when obtaining
     * transaction acknowledgments from Replicas.
     */
    public Durability(SyncPolicy masterSync,
                      SyncPolicy replicaSync,
                      ReplicaAckPolicy replicaAck) {
        this.masterSync = masterSync;
        this.replicaSync = replicaSync;
        this.replicaAck = replicaAck;
    }

    @Override
    public String toString() {
        return masterSync.toString() + "," +
               replicaSync.toString() + "," +
               replicaAck.toString();
    }

    /**
     * Returns the transaction synchronization policy to be used on the Master
     * when committing a transaction.
     */
    public SyncPolicy getMasterSync() {
        return masterSync;
    }

    /**
     * Returns the transaction synchronization policy to be used by the replica
     * as it replays a transaction that needs an acknowledgment.
     */
    public SyncPolicy getReplicaSync() {
        return replicaSync;
    }

    /**
     * Returns the replica acknowledgment policy used by the master when
     * committing changes to a replicated environment.
     */
    public ReplicaAckPolicy getReplicaAck() {
        return replicaAck;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = (prime * result) + masterSync.hashCode();
        result = (prime * result) + replicaAck.hashCode();
        result = (prime * result) + replicaSync.hashCode();
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null) {
            return false;
        }
        if (!(obj instanceof Durability)) {
            return false;
        }
        Durability other = (Durability) obj;
        if (!masterSync.equals(other.masterSync)) {
            return false;
        }
        if (!replicaAck.equals(other.replicaAck)) {
            return false;
        }
        if (!replicaSync.equals(other.replicaSync)) {
            return false;
        }
        return true;
    }
}
