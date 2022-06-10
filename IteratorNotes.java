
//API
QueryIterableResult queryIterable(QueryRequest);

// New classes
QueryIterableResult implements Iterable<MapValue>;
// has methods to return consumption
// doesn't go remote on handle.queryIterable()
// iterator() returns new objects vs Iterator<MapValue>
QueryResultIterator iterator();

QueryResultIterator implements Iterator<MapValue>;
// concerns:
//  API surface area
//  Methods to affect RateLimiter, timeouts, limits
//  Consumption is accumulated in QueryIterableResult
//  Why not just return Iterator<MapValue>

/*
I'd be ok with the current mechanism if we eliminate the public version of
QueryResultIterator. This eliminates the per-iteration config, which is not
helpful. Accumulation of stats and consumption is required regardless
*/

// alternatives

// new object, not part of NoSQLHandle. This makes it clear that it doesn't
// go remote. This would replace NoSQLHandle.queryIterable() and the Iterable
// would not be a Result. This isn't horrible because most/all of the methods
// on Result are internal or hidden.
// QueryIterable would be the old QueryIterableResult and API on that object
// would be the same other than the type of object returned from iterator()
// (it'd be Iterator<MapValue>)
QueryIterable = new QueryIterable(QueryRequest, NoSQLHandle);
