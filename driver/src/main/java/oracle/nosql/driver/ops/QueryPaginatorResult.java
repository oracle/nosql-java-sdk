/*-
 * Copyright (c) 2011, 2025 Oracle and/or its affiliates. All rights reserved.
 *
 * Licensed under the Universal Permissive License v 1.0 as shown at
 *  https://oss.oracle.com/licenses/upl/
 */

package oracle.nosql.driver.ops;

import oracle.nosql.driver.NoSQLHandleAsync;
import oracle.nosql.driver.values.MapValue;

import java.util.List;
import java.util.Objects;
import java.util.concurrent.Flow;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Result for {@link oracle.nosql.driver.NoSQLHandleAsync#queryPaginator}.
 * <p>
 * Pagination is supported through the {@code Flow.Publisher<List<MapValue>>}.
 * Users can subscribe to the publisher return by
 * {@link QueryPaginatorResult#getResults()} to consume the results of the query
 * operation.
 * <p>
 * Note: The read/write KB/Units, rate limit delay and retry stats are summed up
 * from the beginning of the subscription.
 */
public class QueryPaginatorResult extends Result {
    private final QueryPublisher publisher;
    final QueryRequest queryRequest;
    final NoSQLHandleAsync handle;

    final AtomicInteger readKB = new AtomicInteger();
    final AtomicInteger readUnits = new AtomicInteger();
    final AtomicInteger writeKB = new AtomicInteger();
    final AtomicInteger writeUnits = new AtomicInteger();

    public QueryPaginatorResult(QueryRequest queryRequest,
            NoSQLHandleAsync handle) {
        Objects.requireNonNull(queryRequest, "queryRequest should not be null");
        Objects.requireNonNull(handle, "NoSQL handle should not be null");
        if (queryRequest.getContKey() != null) {
            throw new IllegalArgumentException(
                "A new QueryRequest is required for a QueryIterableResult.");
        }
        this.queryRequest = queryRequest;
        this.handle = handle;
        publisher = new QueryPublisher(this);
    }

    public Flow.Publisher<List<MapValue>> getResults() {
        return publisher;
    }

    public int getReadKB() {
        return readKB.get();
    }

    public int getWriteKB() {
        return writeKB.get();
    }

    public int getReadUnits() {
        return readUnits.get();
    }

    public int getWriteUnits() {
        return writeUnits.get();
    }
}
