package com.mongodb.hvdf.test.plugins;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.channels.ChannelInterceptor;

public class AppendArrayInterceptor extends ChannelInterceptor {

	private static final String TARGET_FIELD_KEY = "push_field";
	private static final String PUSH_VALUE_KEY = "push_value";
	
	// The value to add to the array
	private String field  = null; 
	private Object value = new Integer(0); 
	
	@Override
	public void configure(DBObject configuration) {
		
		// get the field and value config
		if(configuration != null){
			field = (String) configuration.get(TARGET_FIELD_KEY);
			value = configuration.get(PUSH_VALUE_KEY);
		}
	}

	@Override
	public void pushSample(DBObject sample, boolean isList, BasicDBList resultList) {
		
		if(sample != null && isList){
			for(Object sampleObj : (BasicDBList)sample){
				pushToDataField((DBObject) sampleObj);
			}
		}
		else{
			pushToDataField((DBObject)sample);
		}
							
		// Call forward the interceptor chain
		this.next.pushSample(sample, isList, resultList);
	}

	
	private void pushToDataField(DBObject sample){
		
		DBObject data = (DBObject) sample.get(Sample.DATA_KEY);
		if(data == null){
			data = new BasicDBObject(field, new BasicDBList());
			sample.put(Sample.DATA_KEY, data);
		}
		
		BasicDBList list = null;
		Object targetObj = data.get(field);
		
		if(targetObj != null && targetObj instanceof BasicDBList){
			list = (BasicDBList) targetObj;
		} else {
			list = new BasicDBList();
			data.put(field, list);
		}
		
		list.add(value);	
	}

}
