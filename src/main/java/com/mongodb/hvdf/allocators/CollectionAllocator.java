package com.mongodb.hvdf.allocators;

import java.util.List;

import com.mongodb.DBCollection;

public interface CollectionAllocator {

	DBCollection getCollection(long timestamp);

	DBCollection getPreviousWithLimit(DBCollection current, long minTime);
	
	List<SliceDetails> getCollectionSlices();
	
}
