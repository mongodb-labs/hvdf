package com.mongodb.hvdf.allocators;


import java.util.ArrayList;
import java.util.List;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.configuration.PluginConfiguration.HVDF;

public class SingleCollectionAllocator implements CollectionAllocator{
		
	private DBCollection collection = null;
	
	public SingleCollectionAllocator(PluginConfiguration config){
		
		String prefix = config.get(HVDF.PREFIX, String.class);
		DB db = config.get(HVDF.DB, DB.class);
		this.collection = db.getCollection(prefix);
	}
	
	@Override
	public DBCollection getCollection(long timestamp) {
		return this.collection;
		
	}

	@Override
	public DBCollection getPreviousWithLimit(DBCollection current, long minTime) {
		
		// There is only one collection, no previous
		return null;
	}

	@Override
	public List<SliceDetails> getCollectionSlices() {

		// Create a list of one !
		List<SliceDetails> result = new ArrayList<SliceDetails>(1);		
		result.add(new SliceDetails(this.collection.getName(), Long.MIN_VALUE, Long.MAX_VALUE));
		return result;
	}
}
