package com.mongodb.hvdf.allocators;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.configuration.PluginConfiguration.HVDF;
import com.mongodb.hvdf.configuration.TimePeriod;

public class PeriodicAllocator implements CollectionAllocator{
	
	private static final String TIME_PERIOD = "period";
	
	private long period = 0;
	private String prefix = null;
	private int prefixLength;
	private DB db = null;
	
	public PeriodicAllocator(PluginConfiguration config){
		
		TimePeriod tPeriod = config.get(TIME_PERIOD, TimePeriod.class);
		period = tPeriod.getAs(TimeUnit.MILLISECONDS);
		prefix = config.get(HVDF.PREFIX, String.class);
		prefixLength = prefix.length();
		db = config.get(HVDF.DB, DB.class);
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
		long currSuffix = getSuffix(currentName);
		
		// If minTime is before the lower bound of this collection,
		// just get the collection with the previous suffix
		if(minTime < currSuffix*this.period){
			return db.getCollection(this.prefix + (currSuffix - 1));
		}

		return null;
	}
	
	@Override
	public List<SliceDetails> getCollectionSlices(){
		
		// get the collection names
		List<SliceDetails> result = new ArrayList<SliceDetails>();
		
		for(String name : this.db.getCollectionNames()){
			if(name.startsWith(this.prefix)){
				long minTime = getSuffix(name)*this.period;
				result.add(new SliceDetails(name, minTime, minTime + this.period - 1));
			}			
		}
		
		return result;
	}
	
	private long getSuffix(final String collName){
		return Long.parseLong(collName.substring(this.prefixLength));
	}

}
