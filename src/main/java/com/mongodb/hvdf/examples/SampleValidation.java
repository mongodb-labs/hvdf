package com.mongodb.hvdf.examples;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.hvdf.api.SampleError;
import com.mongodb.hvdf.api.ServiceException;
import com.mongodb.hvdf.channels.ChannelInterceptor;
import com.mongodb.hvdf.configuration.PluginConfiguration;

public class SampleValidation extends ChannelInterceptor {

	private static final String TARGET_FIELD_KEY = "field_x";
	private static final String MAX_VALUE_CONFIG = "max_value";
	
	// A value of greater than 100 must be clipped to 100
	private int maxValue = 100; 
	
	// A value of zero is illegal
	private static final int illegalValue = 0; 


	protected SampleValidation(PluginConfiguration config) {
			
		// maxValue is configurable, check config for a non-default value
		maxValue = config.get(MAX_VALUE_CONFIG, Integer.class);
	}

	@Override
	public void pushSample(DBObject sample, boolean isList, BasicDBList resultList) {
		
		if(sample != null && isList){
			for(Object sampleObj : (BasicDBList)sample){
				validate((DBObject) sampleObj);
			}
		}
		else{
			validate(sample);
		}
			
		
		// Call forward the interceptor chain
		this.next.pushSample(sample, isList, resultList);
	}
	
	public void validate(DBObject sample){
		
		if(sample != null && sample.containsField(TARGET_FIELD_KEY)){
			int xValue = (Integer)sample.get(TARGET_FIELD_KEY);
			
			// Throw a standard exception if an illegal value is encountered
			if(xValue == illegalValue){
				throw new ServiceException("Illegal value for field_x", 
						SampleError.INVALID_SAMPLE).set(TARGET_FIELD_KEY, xValue);
			}
			
			// Change the value to clip to a configured maximum
			if(xValue > maxValue){
				sample.put(TARGET_FIELD_KEY, maxValue);
			}
		}
		else{
			// The field does not exist
			throw new ServiceException("Sample missing value for field_x", SampleError.INVALID_SAMPLE);
		}		
	}

}
