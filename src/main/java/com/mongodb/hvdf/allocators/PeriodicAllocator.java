package com.mongodb.hvdf.allocators;

import java.util.concurrent.TimeUnit;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.configuration.TimePeriod;

public class PeriodicAllocator implements CollectionAllocator{
	
	private static final String TIME_PERIOD = "period";
	private static final String PREFIX = "prefix";
	private static final String DB = "database";
	
	private long period = 0;
	private String prefix = null;
	private int prefixLength;
	private DB db = null;
	
	public PeriodicAllocator(PluginConfiguration config){
		
		TimePeriod tPeriod = config.get(TIME_PERIOD, TimePeriod.class);
		period = tPeriod.getAs(TimeUnit.MILLISECONDS);
		prefix = config.get(PREFIX, String.class);
		prefixLength = prefix.length();
		db = config.get(DB, DB.class);
	}
	

	@Override
	public DBCollection getCollection(long timestamp) {
		String collName = prefix + timestamp/period;
		return db.getCollection(collName);
		
	}

	@Override
	public DBCollection getPreviousWithLimit(DBCollection current, long minTime) {
		
		// figure out the current timeslice
		String currentName = current.getName();
		long currSuffix = Long.parseLong(currentName.substring(this.prefixLength));
		
		// If minTime is before the lower bound of this collection,
		// just get the collection with the previous suffix
		if(minTime < currSuffix*this.period){
			return db.getCollection(this.prefix + (currSuffix - 1));
		}

		return null;
	}

}
