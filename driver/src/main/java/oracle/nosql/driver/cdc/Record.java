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

    /*
     * @hidden
     */
    public Record() {}

    /*
     * @hidden
     */
	public Record(String eventId,
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

    public String getEventId() {
		return eventId;
	}

    public MapValue getRecordKey() {
		return recordKey;
	}

    public Image getCurrentImage() {
		return currentImage;
	}

    public Image getBeforeImage() {
		return beforeImage;
	}

    public long getModificationTime() {
		return modificationTime;
	}

    public long getExpirationTime() {
		return expirationTime;
	}

    public int getPartitionId() {
		return partitionId;
	}

    public int getRegionId() {
		return regionId;
	}

    /*
     * @hidden
     */
    public void setEventId(String eventId) {
		this.eventId = eventId;
	}

    /*
     * @hidden
     */
    public void setRecordKey(MapValue recordKey) {
		this.recordKey = recordKey;
	}

    /*
     * @hidden
     */
    public void setCurrentImage(Image image) {
		this.currentImage = image;
	}

    /*
     * @hidden
     */
    public void setBeforeImage(Image image) {
		this.beforeImage = image;
	}

    /*
     * @hidden
     */
    public void setModificationTime(long time) {
		this.modificationTime = time;
	}

    /*
     * @hidden
     */
    public void setExpirationTime(long time) {
		this.expirationTime = time;
	}

    /*
     * @hidden
     */
    public void setPartitionId(int pid) {
		this.partitionId = pid;
	}

    /*
     * @hidden
     */
    public void setRegionId(int rid) {
		this.regionId = rid;
	}
}
