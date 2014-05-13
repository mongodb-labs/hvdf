package com.mongodb.hvdf.allocators;

import com.mongodb.DBCollection;

public interface CollectionAllocator {

	DBCollection getCollection(long timestamp);

	DBCollection getPreviousWithLimit(DBCollection current, long minTime);
}
