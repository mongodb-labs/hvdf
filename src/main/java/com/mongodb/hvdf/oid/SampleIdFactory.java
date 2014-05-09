package com.mongodb.hvdf.oid;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;

public interface SampleIdFactory {
	
	SampleId createId(long sourceId, long timeStamp);
	SampleId createId(ObjectId docId);
	
	BasicDBObject limitIdTimeRange(long sourceId, long timeStart, long minTime); 

}
