package com.mongodb.hvdf.rollup;

import java.util.ArrayList;
import java.util.List;
import java.util.TreeSet;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hvdf.api.ConfigurationError;
import com.mongodb.hvdf.api.ServiceException;
import com.mongodb.hvdf.configuration.PluginConfiguration;

public class GroupCountRollup extends RollupOperation {
	
    protected final String fieldExtention;
	protected final List<RangeCounter> ranges;

	public GroupCountRollup(PluginConfiguration config){
		
		this.fieldExtention = "." + "group_count";
		
		// any field mentioned in the config is a target
		DBObject rawConfig = config.getRaw();
		ranges = new ArrayList<RangeCounter>(rawConfig.keySet().size());
		
		for(String fieldName : rawConfig.keySet()){
			Object rangesObj = rawConfig.get(fieldName);
			if(rangesObj instanceof BasicDBList){
				ranges.add(new RangeCounter(fieldName, (BasicDBList)rangesObj));				
			} else {
				// the field must specify a range of values
				throw new ServiceException("Expected a list of range values", 
						ConfigurationError.INVALID_CONFIG_TYPE).
							set("configuring", GroupCountRollup.class).
							set("field", fieldName);	
			}
		}		
	}

	@Override
	public DBObject getUpdateClause(DBObject sample) {

		BasicDBObject incFields = new BasicDBObject();
		for(RangeCounter counter : ranges){
			
			Object valueObj = getNestedFieldValue(counter.getFieldName(), sample);
			if(valueObj != null){
				if (valueObj instanceof DBObject){					
					// this might be a nested *do everything* request
					// TODO : handle recursing into the object adding all fields
					
				} else {
					// this is the normal case, we can apply it directly as a clause
					incFields.append(counter.getIncField(valueObj), 1);
				}
			}
		}
		
		if(incFields.size() > 0){
			return new BasicDBObject("$inc", incFields);
		} else {
			return new BasicDBObject();
		}
	}	
}

class RangeCounter{

    private static Logger logger = LoggerFactory.getLogger(RangeCounter.class);

    private final String fieldName;
    private final String fieldNamePrefix;
	private final TreeSet<Object> rangesValues;
	
	public RangeCounter(String fieldName, BasicDBList rangesObj) {
		this.fieldName = fieldName;
		this.fieldNamePrefix = fieldName + ".group_count.";
		this.rangesValues = new TreeSet<Object>();
		this.rangesValues.addAll(rangesObj);
	}
	
	public String getFieldName() {
		return this.fieldName;
	}

	public String getIncField(Object value){
		
		if(value != null){
			try{
				Object floor = rangesValues.floor(value);
				if(floor != null){
					return this.fieldNamePrefix + floor.toString();
				}
			} catch(Exception e) {
				logger.warn("Value {} for field {} not compatible with ranges", value, this.fieldNamePrefix);
			}
		}
		
		// Doesnt fit any range, its in the low category
		return this.fieldNamePrefix + "low";
	}
	
}	
