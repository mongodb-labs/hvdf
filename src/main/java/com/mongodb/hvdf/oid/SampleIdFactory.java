package com.mongodb.hvdf.oid;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;

public interface SampleIdFactory {
	
	SampleId createId(DBObject sample); 
	SampleId createId(Object sourceId, long timeStamp);
	SampleId createId(ObjectId docId);
	
	BasicDBObject getTimeRangeQuery(Object sourceId, long timeStart, long minTime);
	BasicDBObject getQuery(Object sourceId, long timestamp);

}
