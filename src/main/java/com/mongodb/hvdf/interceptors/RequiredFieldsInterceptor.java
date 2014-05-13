package com.mongodb.hvdf.interceptors;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.api.SampleError;
import com.mongodb.hvdf.api.ServiceException;
import com.mongodb.hvdf.channels.ChannelInterceptor;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.oid.SampleId;
import com.mongodb.hvdf.oid.SampleIdFactory;

public class RequiredFieldsInterceptor extends ChannelInterceptor{

	private final SampleIdFactory idFactory;
	
	
	public RequiredFieldsInterceptor(
			PluginConfiguration config, 
			final SampleIdFactory idFactory){
		this.idFactory = idFactory;
	}
		
	@Override
	public void pushSample(DBObject sample, boolean isList, BasicDBList resultIds) {
		
		if(sample != null && isList){
			for(Object sampleObj : (BasicDBList)sample){
				validate((BasicDBObject) sampleObj, resultIds);
			}
		}
		else{
			validate((BasicDBObject)sample, resultIds);
		}
			
		
		// Call forward the interceptor chain
		this.next.pushSample(sample, isList, resultIds);
		
	}
	
	private void validate(BasicDBObject document, BasicDBList resultIds){
		
		if(document != null){
			
			// First check/retrieve the timestamp
			long timestamp = 0;
			Object tsObj = document.get(Sample.TS_KEY);
			if(tsObj == null){
				timestamp = System.currentTimeMillis();
				document.put(Sample.TS_KEY, timestamp);
			}
			else{
				try{
					// Timestamp must be a Long
					timestamp = (Long)tsObj;					
				
				} catch(ClassCastException ccex){
					
					try{
						// If not a Long, but other Number, convert
						timestamp = ((Number)tsObj).longValue();
						document.put(Sample.TS_KEY, timestamp);		
					} catch(Exception ex){
						throw new ServiceException("Illegal type for timestamp", 
								SampleError.INVALID_SAMPLE).set(Sample.TS_KEY, tsObj);
					}
				}
			}
			
			// Now use the ID strategy to 
			// Create an oid to embed the sample time
			if(idFactory != null){
				SampleId _id = idFactory.createId(0L, timestamp);
				document.put(Sample.ID_KEY, _id.toObjectId());
				resultIds.add(_id.toObjectId());
			}
		}					
	}
	
	
	
}
