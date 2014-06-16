package com.mongodb.hvdf.rollup;

import com.mongodb.BasicDBObject;
import com.mongodb.hvdf.configuration.PluginConfiguration;

public class CountRollup extends OperatorBasedRollup{
	
	public CountRollup(PluginConfiguration config){
		super(config, "$inc", "count");
	}

	@Override
	protected void appendField(
			BasicDBObject opFields, String fieldName, Object valueObj) {
		
		// Override the base since the default behavior would $inc by the
		// value but in this case we just want to inc by 1 for a count
		opFields.append(fieldName + this.fieldExtention, 1);		
	}

}
