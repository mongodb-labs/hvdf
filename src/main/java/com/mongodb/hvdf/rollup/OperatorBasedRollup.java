package com.mongodb.hvdf.rollup;

import java.util.Set;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hvdf.configuration.PluginConfiguration;

public abstract class OperatorBasedRollup extends RollupOperation{
	
	protected final Set<String> rawFields;
	protected final String fieldExtention;
	protected final String operator;

	public OperatorBasedRollup(PluginConfiguration config, String operator, String rolledFieldName){
		
		this.operator = operator;
		this.fieldExtention = "." + rolledFieldName;
		// any field mentioned in the config is a target
		this.rawFields = config.getRaw().keySet();
	}

	@Override
	public DBObject getUpdateClause(DBObject sample) {

		BasicDBObject opFields = new BasicDBObject();
		for(String fieldName : rawFields){
			
			Object valueObj = getNestedFieldValue(fieldName, sample);
			if(valueObj != null){
				if (valueObj instanceof DBObject){					
					// this might be a nested *do everything* request
					// TODO : handle recursing into the object adding all fields
					
				} else {
					// this is the normal case, we can apply it directly as a clause
					appendField(opFields, fieldName, valueObj);
				}
			}
		}
		
		if(opFields.size() > 0){
			return getOperator(opFields);
		} else {
			return new BasicDBObject();
		}
	}

	protected  DBObject getOperator(BasicDBObject opFields){
		return new BasicDBObject(this.operator, opFields);
	}

	protected void appendField(
			BasicDBObject opFields, String fieldName, Object valueObj){
		opFields.append(fieldName + fieldExtention, valueObj);				
	}

}
