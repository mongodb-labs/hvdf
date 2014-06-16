package com.mongodb.hvdf.rollup;

import com.mongodb.DBObject;

public abstract class RollupOperation {
			
	public abstract DBObject getUpdateClause(DBObject sample);
	
	protected static Object getNestedFieldValue(String fieldName, DBObject sample) {
		
		Object levelValue = sample;
		String[] levels = fieldName.split("\\.");
		for(String levelName : levels){
			if(levelValue instanceof DBObject){
				levelValue = ((DBObject)levelValue).get(levelName);
			} else {
				return null;
			}
		}
		
		return levelValue;
	}	
}
