package com.mongodb.hvdf.oid;

import org.bson.types.ObjectId;

public interface SampleId {
	
	boolean embedsTime();
	boolean embedsSource();
	
	long getTime();
	long getSourceId();
	
	ObjectId toObjectId();

}
