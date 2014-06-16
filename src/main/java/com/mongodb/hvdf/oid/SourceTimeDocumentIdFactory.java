package com.mongodb.hvdf.oid;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.configuration.PluginConfiguration;

public class SourceTimeDocumentIdFactory implements SampleIdFactory{

	private static final String ID_SOURCE_KEY = Sample.ID_KEY + "." + Sample.SOURCE_KEY;
	private static final String ID_TIME_KEY = Sample.ID_KEY + "." + Sample.TS_KEY;
	
	public SourceTimeDocumentIdFactory(PluginConfiguration config){}

	@Override
	public SampleId createId(DBObject sample) {
		
		// get the timestamp as a long
		return createId(sample.get(Sample.SOURCE_KEY), (Long)sample.get(Sample.TS_KEY));
	}

	@Override
	public SampleId createId(Object sourceId, long timeStamp) {
		
		return new SourceTimeDocumentId(sourceId, timeStamp);
	}

	@Override
	public SampleId createId(ObjectId docId) {

		return new SourceTimeDocumentId(docId);
	}

	@Override
	public BasicDBObject getTimeRangeQuery(
			Object sourceId, long timeStart, long minTime) {
				
		if(sourceId == null){
			// All sources across the time range, cannot use the id index
			return new BasicDBObject(ID_TIME_KEY, new BasicDBObject("$lte", timeStart).append("$gte", minTime));	
			
		
		} else if( sourceId instanceof BasicDBList){
			
			// Create a query from the id components rather than the id itself
			return new BasicDBObject(ID_SOURCE_KEY, new BasicDBObject("$in", sourceId)).append(
					ID_TIME_KEY, new BasicDBObject("$lte", timeStart).append("$gte", minTime));	
		} else {
			
			// Single sourceId, construct a more optimal ID query for this case
			BasicDBObject startId = new BasicDBObject(
					Sample.SOURCE_KEY, sourceId).append(Sample.TS_KEY, timeStart);
			BasicDBObject minId = new BasicDBObject(
					Sample.SOURCE_KEY, sourceId).append(Sample.TS_KEY, minTime);
			
			return new BasicDBObject("$lte", startId).append("$gte", minId);
		}		
	}

	@Override
	public BasicDBObject getQuery(Object sourceId, long timestamp) {
		
		if(sourceId == null){
			// query by the time component of _id
			return new BasicDBObject(ID_TIME_KEY, timestamp);
			
		} else if(sourceId instanceof BasicDBList){
			// make a list of source/time id values
			BasicDBList idList = new BasicDBList();
			for(Object sourceItem : (BasicDBList)sourceId){
				idList.add(new BasicDBObject(Sample.SOURCE_KEY, sourceItem)
					.append(Sample.TS_KEY, timestamp));
			}			
			return new BasicDBObject(Sample.ID_KEY, new BasicDBObject("$in", idList));
			
		} else {
			
			// single sourceId, looking for a specific _id value
			BasicDBObject targetId= new BasicDBObject(
					Sample.SOURCE_KEY, sourceId).append(Sample.TS_KEY, timestamp);
			
			return new BasicDBObject(Sample.ID_KEY, targetId);
		}
	}
}
