package com.mongodb.hvdf.oid;

import org.bson.types.ObjectId;

import com.mongodb.BasicDBObject;

public class HiDefTimeIdFactory implements SampleIdFactory{

	
	@Override
	public SampleId createId(long sourceId, long timeStamp) {
		
		return new HiDefTimeSampleId(timeStamp);
	}

	@Override
	public SampleId createId(ObjectId docId) {

		return new HiDefTimeSampleId(docId);
	}

	@Override
	public BasicDBObject limitIdTimeRange(
			long sourceId, long timeStart, long minTime) {
		
		
		byte[] startId = new byte[12];
		byte[] minId = new byte[12];
		HiDefTimeSampleId.writeTimeStamp(startId, timeStart + 1);
		HiDefTimeSampleId.writeTimeStamp(minId, minTime + 1);
		
		return new BasicDBObject("$lt", new ObjectId(startId)).
				append("$gte", new ObjectId(minId));
	}
	
}
