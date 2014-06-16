package com.mongodb.hvdf.oid;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.configuration.PluginConfiguration;

public class HiDefTimeIdFactory implements SampleIdFactory{

	
	public HiDefTimeIdFactory(PluginConfiguration config){}

	@Override
	public SampleId createId(DBObject sample) {
		
		// get the timestamp as a long
		return createId(null, (Long)sample.get(Sample.TS_KEY));
	}

	@Override
	public SampleId createId(Object sourceId, long timeStamp) {
		
		return new HiDefTimeSampleId(timeStamp);
	}

	@Override
	public SampleId createId(ObjectId docId) {

		return new HiDefTimeSampleId(docId);
	}

	@Override
	public BasicDBObject getTimeRangeQuery(
			Object sourceId, long timeStart, long minTime) {
		
		byte[] startId = new byte[12];
		byte[] minId = new byte[12];
		HiDefTimeSampleId.writeTimeStamp(startId, timeStart + 1);
		HiDefTimeSampleId.writeTimeStamp(minId, minTime + 1);
		
		BasicDBObject idTimeRange =  new BasicDBObject("$lt", new ObjectId(startId)).
				append("$gte", new ObjectId(minId));
		
		BasicDBObject query = new BasicDBObject(Sample.ID_KEY, idTimeRange);
		
		// There is no source embedded, so we need a general clause for sourceId
		addStandardSourceClause(query, sourceId);		
		
		return query;
	}

	@Override
	public BasicDBObject getQuery(Object sourceId, long timestamp) {
		byte[] minId = new byte[12];
		byte[] maxId = new byte[12];
		HiDefTimeSampleId.writeTimeStamp(minId, timestamp);
		HiDefTimeSampleId.writeTimeStamp(maxId, timestamp + 1);
		
		BasicDBObject idTimeRange = new BasicDBObject("$lt", new ObjectId(maxId)).
				append("$gte", new ObjectId(minId));
		
		BasicDBObject query = new BasicDBObject(Sample.ID_KEY, idTimeRange);
		
		// There is no source embedded, so we need a general clause for sourceId
		addStandardSourceClause(query, sourceId);		
		
		return query;
	}

	private void addStandardSourceClause(BasicDBObject query, Object sourceId) {
		
		if(sourceId != null){
			if(sourceId instanceof BasicDBList){
				// The the source provided is a list, convert to $in
				query.append(Sample.SOURCE_KEY, new BasicDBObject("$in", sourceId));
			} else {
				// Its an absolute value
				query.append(Sample.SOURCE_KEY, sourceId);
			}
		}
	}
	
	
}
