/*-
 * Copyright (c) 2011, 2026 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.changestream;

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

    private String tableName;
    private String compartmentOcid;
    private String tableOcid;

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
    public Record(String tableName,
                 String compartmentOcid,
                 String tableOcid,
                 String eventId,
                 MapValue recordKey,
                 Image currentImage,
                 Image beforeImage,
                 long modificationTime,
                 long expirationTime,
                 int partitionId,
                 int regionId) {
        this.tableName = tableName;
        this.compartmentOcid = compartmentOcid;
        this.tableOcid = tableOcid;
        this.eventId = eventId;
        this.recordKey = recordKey;
        this.currentImage = currentImage;
        this.beforeImage = beforeImage;
        this.modificationTime = modificationTime;
        this.expirationTime = expirationTime;
        this.partitionId = partitionId;
        this.regionId = regionId;
    }

    /* Get the table name for this record. */
    public String getTableName() {
        return tableName;
    }

    /*
     * Get the compartment Ocid for this record. If this is empty,
     * the compartment is assumed to be the default compartment
     * for the tenancy.
     */
    public String getCompartmentOcid() {
        return compartmentOcid;
    }

    /* Get the table Ocid for this record. */
    public String getTableOcid() {
        return tableOcid;
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Record {\n");
        sb.append(" tableName: { ").append(tableName).append(" }\n");
        sb.append(" compartmentOcid: { ").append(compartmentOcid).append(" }\n");
        sb.append(" tableOcid: { ").append(tableOcid).append(" }\n");
        sb.append(" eventId: { ").append(eventId).append(" }\n");
        sb.append(" recordKey: { ").append(recordKey).append(" }\n");
        sb.append(" currentImage: { ").append(currentImage).append(" }\n");
        sb.append(" beforeImage: { ").append(beforeImage).append(" }\n");
        sb.append(" modificationTime: { ").append(modificationTime).append(" }\n");
        sb.append(" expirationTime: { ").append(expirationTime).append(" }\n");
        sb.append(" partitionId: { ").append(partitionId).append(" }\n");
        sb.append("}");
        return sb.toString();
    }
}
