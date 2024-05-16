/*-
 * Copyright (c) 2011, 2024 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver;

import static oracle.nosql.driver.util.CheckNull.requireNonNull;

import oracle.nosql.driver.http.NoSQLHandleAsyncImpl;
import oracle.nosql.driver.http.NoSQLHandleImpl;

/**
 * Factory class used to produce handles to operate on tables.
 */
public class NoSQLHandleFactory {

    /**
     * Creates a handle that can be used to access tables. The application must
     * invoke {@link NoSQLHandle#close}, when it is done accessing the system to
     * free up resources associated with the handle.
     *
     * @param config the NoSQLHandle configuration parameters
     *
     * @return a valid {@link NoSQLHandle} instance, ready for use
     *
     * @throws IllegalArgumentException if an illegal configuration parameter
     * is specified.
     *
     * @see NoSQLHandle#close
     */
    public static NoSQLHandle createNoSQLHandle(NoSQLHandleConfig config) {
        requireNonNull(
            config,
            "NoSQLHandleFactory.createNoSQLHandle: config cannot be null");
        NoSQLHandleConfig configCopy = config.clone();
        if (configCopy.getRetryHandler() == null) {
            /*
             * Default retry handler: 10 retries, default backoff
             */
            configCopy.configureDefaultRetryHandler(10, 0);
        }
        return new NoSQLHandleImpl(configCopy);
    }

    /**
     * Creates a handle that can be used to access tables asynchronously. The
     * application
     * must
     * invoke {@link NoSQLHandleAsync#close}, when it is done accessing the
     * system to
     * free up resources associated with the handle.
     *
     * @param config the NoSQLHandle configuration parameters
     *
     * @return a valid {@link NoSQLHandleAsync} instance, ready for use
     *
     * @throws IllegalArgumentException if an illegal configuration parameter
     * is specified.
     *
     * @see NoSQLHandleAsync#close
     */
    public static NoSQLHandleAsync createNoSQLHandleAsync(NoSQLHandleConfig config) {
        requireNonNull(
                config,
                "NoSQLHandleFactory.createNoSQLHandle: config cannot be null");
        NoSQLHandleConfig configCopy = config.clone();
        if (configCopy.getRetryHandler() == null) {
            /*
             * Default retry handler: 10 retries, default backoff
             */
            configCopy.configureDefaultRetryHandler(10, 0);
        }
        return new NoSQLHandleAsyncImpl(configCopy);
    }

}
