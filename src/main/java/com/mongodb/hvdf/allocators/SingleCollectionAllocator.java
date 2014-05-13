package com.mongodb.hvdf.allocators;


import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.hvdf.configuration.PluginConfiguration;

public class SingleCollectionAllocator implements CollectionAllocator{
	
	private static final String PREFIX = "prefix";
	private static final String DB = "database";
	
	private DBCollection collection = null;
	
	public SingleCollectionAllocator(PluginConfiguration config){
		
		String prefix = config.get(PREFIX, String.class);
		DB db = config.get(DB, DB.class);
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
}
