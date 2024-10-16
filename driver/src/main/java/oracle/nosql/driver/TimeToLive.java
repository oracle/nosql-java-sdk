/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import java.util.concurrent.TimeUnit;

/**
 * TimeToLive is a utility class that represents a period of time, similar to
 * java.time.Duration in Java, but specialized to the needs of this driver.
 * <p>
 * This class is restricted to durations of days and hours. It is only
 * used as input related to time to live (TTL) for row instances.
 * Construction allows only day and hour
 * durations for efficiency reasons. Durations of days are recommended as they
 * result in the least amount of storage overhead.
 * <p>
 * Only positive durations are allowed on input, although negative durations
 * can be returned from {@link #fromExpirationTime} if the expirationTime is
 * in the past relative to the referenceTime.
 */
public final class TimeToLive {

    private final long value;
    private final TimeUnit unit;

    /**
     * A convenience constant that can be used as an argument to
     * indicate that the row should not expire.
     */
    public static final TimeToLive DO_NOT_EXPIRE = ofDays(0);

    /**
     * Creates a duration using a period of hours.
     *
     * @param hours the number of hours in the duration, must be
     * a non-negative number
     *
     * @return the duration
     *
     * @throws IllegalArgumentException if a negative value is provided
     */
    public static TimeToLive ofHours(long hours) {
        if (hours < 0) {
            throw new IllegalArgumentException(
                "TimeToLive does not support negative time periods");
        }
        return new TimeToLive(hours, TimeUnit.HOURS);
    }

    /**
     * Creates a duration using a period of 24 hour days.
     *
     * @param days the number of days in the duration, must be
     * a non-negative number
     *
     * @return the duration
     *
     * @throws IllegalArgumentException if a negative value is provided
     */
    public static TimeToLive ofDays(long days) {
        if (days < 0) {
            throw new IllegalArgumentException(
                "TimeToLive does not support negative time periods");
        }
        return new TimeToLive(days, TimeUnit.DAYS);
    }

    /**
     * Returns the number of days in this duration, which may be negative.
     *
     * @return the number of days
     */
    public long toDays() {
        return TimeUnit.DAYS.convert(value, unit);
    }

    /**
     * Returns the number of hours in this duration, which may be negative.
     *
     * @return the number of hours
     */
    public long toHours() {
        return TimeUnit.HOURS.convert(value, unit);
    }

    /**
     * Returns an absolute time representing the duration plus the absolute
     * time reference parameter. If an expiration time from the current time is
     * desired the parameter should be {@link System#currentTimeMillis}. If the
     * duration of this object is 0 ({@link #DO_NOT_EXPIRE}), indicating no
     * expiration time, this method will return 0, regardless of the reference
     * time.
     *
     * @param referenceTime an absolute time in milliseconds since January
     * 1, 1970.
     *
     * @return time in milliseconds, 0 if this object's duration is 0
     */
    public long toExpirationTime(long referenceTime) {
        if (value == 0) {
            return 0;
        }
        return referenceTime + toMillis();
    }

    /**
     * Returns an instance of TimeToLive based on an absolute expiration
     * time and a reference time. If a duration relative to the current time
     * is desired the referenceTime should be {@link System#currentTimeMillis}.
     * If the expirationTime is 0, the referenceTime is ignored and a
     * TimeToLive of duration 0 is created, indicating no expiration.
     * <p>
     * Days will be use as the primary unit of duration if the expiration time
     * is evenly divisible into days, otherwise hours are used.
     *
     * @param expirationTime an absolute time in milliseconds since January
     * 1, 1970
     *
     * @param referenceTime an absolute time in milliseconds since January
     * 1, 1970.
     *
     * @return a new TimeToLive instance
     */
    public static TimeToLive fromExpirationTime(long expirationTime,
                                                long referenceTime) {
        final long MILLIS_PER_HOUR = 1000L * 60 * 60;

        if (expirationTime == 0) {
            return DO_NOT_EXPIRE;
        }

        /*
         * Calculate whether the given time in millis, when converted to hours,
         * rounding up, is not an even multiple of 24.
         */
        final long hours =
            (expirationTime + MILLIS_PER_HOUR - 1) / MILLIS_PER_HOUR;
        boolean timeInHours = hours % 24 != 0;

        /*
         * This may result in a negative duration. This is ok and is documented.
         * If somehow the duration is 0, set it to -1 hours because 0 means
         * no expiration.
         */
        long duration = expirationTime - referenceTime;

        if (duration == 0) {
            /* very unlikely, but possible; set to -1 hours to avoid 0 */
            duration = -MILLIS_PER_HOUR;
            timeInHours = true;
        }
        if (timeInHours) {
            if (duration > 0 && duration < MILLIS_PER_HOUR) {
                return new TimeToLive(1, TimeUnit.HOURS);
            }
            return new TimeToLive(
                TimeUnit.HOURS.convert(duration, TimeUnit.MILLISECONDS),
                TimeUnit.HOURS);
        }
        if (duration > 0 && duration < MILLIS_PER_HOUR * 24) {
            return new TimeToLive(1, TimeUnit.DAYS);
        }
        return new TimeToLive(
                TimeUnit.DAYS.convert(duration, TimeUnit.MILLISECONDS),
                TimeUnit.DAYS);
    }

    @Override
    public String toString() {
        return value + " " + unit.toString();
    }

    /**
     * Returns the numeric duration value
     *
     * @return the duration value, independent of unit
     */
    public long getValue() {
        return value;
    }

    /**
     * Returns the TimeUnit used for the duration
     *
     * @return the unit
     */
    public TimeUnit getUnit() {
        return unit;
    }

    /**
     * @hidden
     * @return true if unit is days
     */
    public boolean unitIsDays() {
        return unit == TimeUnit.DAYS;
    }

    /**
     * @hidden
     * @return true if unit is hours
     */
    public boolean unitIsHours() {
        return unit == TimeUnit.HOURS;
    }

    /**
     * Private constructor. All construction is done via this constructor, which
     * validates the arguments.
     *
     * @param value value of time
     * @param unit unit of time, cannot be null
     */
    private TimeToLive(long value, TimeUnit unit) {

        if (unit != TimeUnit.DAYS && unit != TimeUnit.HOURS) {
            throw new IllegalArgumentException(
                "Invalid TimeUnit (" + unit + ") in TimeToLive construction." +
                "Must be DAYS or HOURS.");
        }

        this.value = value;
        this.unit = unit;
    }

    private long toMillis() {
        return TimeUnit.MILLISECONDS.convert(value, unit);
    }
}
