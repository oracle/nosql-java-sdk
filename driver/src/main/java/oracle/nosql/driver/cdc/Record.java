/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.cdc;

import oracle.nosql.driver.values.MapValue;

/**
 * A single change record.
 * It contains the current and previous record images, plus
 * the record key and data for the change event.
 *
 * PUT operations will always have a non-null currentImage and may have
 * a non-null beforeImage.
 *
 * DELETE operations will have a null currentImage and may
 * have a non-null beforeImage.
 */
public class Record {
	/* event ID: this is unique within the consumer group */
    private String eventId;

    /*
	 * Record key. Note that the key fields are *not* present in
     * the change images.
     */
    private MapValue recordKey;

    /*
     * The current value of the record, if any.
     */
    private Image currentImage;

    /*
     * The previous value of the record, if any.
     */
    private Image beforeImage;

    private long modificationTime; // ms since the epoch
    private long expirationTime; // ms since the epoch
    private int partitionId;
    private int regionId;

	Record(String eventId,
                 MapValue recordKey,
                 Image currentImage,
                 Image beforeImage,
                 long modificationTime,
                 long expirationTime,
                 int partitionId,
                 int regionId) {
	    this.eventId = eventId;
        this.recordKey = recordKey;
        this.currentImage = currentImage;
        this.beforeImage = beforeImage;
        this.modificationTime = modificationTime;
        this.expirationTime = expirationTime;
        this.partitionId = partitionId;
        this.regionId = regionId;
	}

    String getEventId() {
		return eventId;
	}

    MapValue getRecordKey() {
		return recordKey;
	}

    Image getCurrentImage() {
		return currentImage;
	}

    Image getBeforeImage() {
		return beforeImage;
	}

    long getModificationTime() {
		return modificationTime;
	}

    long getExpirationTIme() {
		return expirationTime;
	}

    int getPartitionId() {
		return partitionId;
	}

    int getRegionId() {
		return regionId;
	}
}
