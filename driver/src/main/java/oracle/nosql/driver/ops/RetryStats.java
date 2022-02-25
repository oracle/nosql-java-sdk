/*-
 * Copyright (c) 2011, 2022 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import java.util.Map;
import java.util.HashMap;

import oracle.nosql.driver.RetryHandler;

/**
 * A class that maintains stats on retries during a request.
 *
 * This object tracks statistics about retries performed during
 * requests. It can be accessed from within retry handlers
 * (see {@link RetryHandler}) or after a request is finished by calling
 * {@link Request#getRetryStats}.
 */
public class RetryStats {

    /* total number of retries in this request */
    private int retries;

    /* amount of time, in millis, spent delaying in retry handling */
    private int delayMs;

    /* map of exception type --> number of exceptions during this request */
    private Map<Class<? extends Throwable>, Integer> exceptionMap;

    /**
     * @hidden
     * Internal use only.
     * Create a new retry stats object.
     */
    RetryStats() {
        this.retries = 0;
        this.delayMs = 0;
        this.exceptionMap = new HashMap<Class<? extends Throwable>, Integer>();
    }

    /**
     * @hidden
     * Internal use only.
     * Adds an exception class to the stats object.
     * This increments the exception count and adds to the count of
     * this type of exception class.
     * @param e the exception class
     */
    public void addException(Class<? extends Throwable> e) {
        addException(e, 1);
    }

    /**
     * @hidden
     * Internal use only.
     * Adds an exception class to the stats object.
     * This increments the exception count and adds to the count of
     * this type of exception class.
     * @param e the exception class
     * @param n the number of such exceptions
     */
    public void addException(Class<? extends Throwable> e, int n) {
        int i = getNumExceptions(e) + n;
        exceptionMap.put(e, i);
    }

    /**
     * @hidden
     * Internal use only.
     * Adds an exception class to the stats object.
     * This increments the exception count and adds to the count of
     * this type of exception class.
     * @param e the exception class
     */
    public void addExceptions(Map<Class<? extends Throwable>, Integer> e) {
        for (Map.Entry<Class<? extends Throwable>, Integer> entry :
            e.entrySet()) {
            addException(entry.getKey(), entry.getValue());
        }
    }

    /**
     * @hidden
     * Internal use only.
     * Returns the map of exceptions and their count.
     */
    Map<Class<? extends Throwable>, Integer> getExceptionMap() {
        return exceptionMap;
    }

    /**
     * @hidden
     * Internal use only.
     * Adds time to the overall delay time spent.
     * @param d number of milliseconds to add to the delay total
     */
    public void addDelayMs(int d) {
        delayMs += d;
    }

    /**
     * @hidden
     * Internal use only.
     * Increments the number of retries.
     */
    public void incrementRetries() {
        retries++;
    }

    /**
     * @hidden
     * Internal use only.
     * Increments the number of retries with n amount.
     */
    public void incrementRetries(int n) {
        retries += n;
    }

    /**
     * Returns the number of exceptions of a particular class.
     * If no exceptions of this class were added to this stats object,
     * the return value is zero.
     * @param e the class of exception to query
     * @return the number of exceptions of this class
     */
    public int getNumExceptions(Class<? extends Throwable> e) {
        Integer i = exceptionMap.get(e);
        if (i == null) {
            return 0;
        }
        return i.intValue();
    }

    /**
     * Returns the total time delayed (slept) between retry events.
     * @return time delayed during retries, in milliseconds. This is only
     *         the time spent locally in sleep() between retry events.
     */
    public int getDelayMs() {
        return delayMs;
    }

    /**
     * Returns the number of retry events.
     * @return number of retry events
     */
    public int getRetries() {
        return retries;
    }

    /**
     * @hidden
     * Internal use only.
     * Clears the stats object.
     */
    public void clear() {
        delayMs = 0;
        retries = 0;
        exceptionMap.clear();
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("retries=").append(retries)
            .append(" delayMs=").append(delayMs)
            .append(" exceptionMap=").append(exceptionMap);
        return sb.toString();
    }
}
