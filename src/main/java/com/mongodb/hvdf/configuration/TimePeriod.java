package com.mongodb.hvdf.configuration;

import java.util.HashMap;
import java.util.concurrent.TimeUnit;

import com.mongodb.DBObject;

public class TimePeriod {
	
	private static final HashMap<String,Long> timeValues = new HashMap<String,Long>();
	
	static{
		
		// All valid time units used in config 
		timeValues.put("milliseconds",  1L);
		timeValues.put("millisecond",   1L);
		timeValues.put("seconds",       1000L);
		timeValues.put("second",        1000L);
		timeValues.put("minutes",       60*1000L);
		timeValues.put("minute",        60*1000L);
		timeValues.put("hours",         60*60*1000L);
		timeValues.put("hour",          60*60*1000L);
		timeValues.put("days",          24*60*60*1000L);
		timeValues.put("day",           24*60*60*1000L);
		timeValues.put("weeks",         7*24*60*60*1000L);
		timeValues.put("week",          7*24*60*60*1000L);
		timeValues.put("years",         365*24*60*60*1000L);
		timeValues.put("year",          365*24*60*60*1000L);		
	}
	
	// Internal ms representation
	private long msValue = 0;
	
	
	public TimePeriod(Object rawItem){
		
		// if the value is simply a number then its straight ms
		if(rawItem instanceof Number){
			msValue = ((Number) rawItem).longValue();
		}
		else if(rawItem instanceof DBObject){
			processDocument((DBObject)rawItem);
		}
		else{
			// Treat this as a failure to "cast" the config
			// value to a time period
			throw new ClassCastException();
		}
	}
	
	public long getAs(TimeUnit unit){
		return unit.convert(msValue, TimeUnit.MILLISECONDS);
	}

	private void processDocument(DBObject rawItem) {
				
		for(String key : rawItem.keySet()){
			
			Long entryValue = timeValues.get(key.toLowerCase());
			if(entryValue != null){
				// If the value is a number get the unit qty
				Long qty = ((Number) rawItem.get(key)).longValue();
				
				// Total up the units and quantities
				msValue += qty*entryValue;
				
			} else {
				// Key must be a valid time unit
				throw new ClassCastException();
			}			
		}		
	}
	
	
	
	

}
