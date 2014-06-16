package com.mongodb.hvdf.oid;

public interface SampleId {
	
	boolean embedsTime();
	boolean embedsSource();
	
	long getTime();
	Object getSourceId();
	
	Object toObject();

}
