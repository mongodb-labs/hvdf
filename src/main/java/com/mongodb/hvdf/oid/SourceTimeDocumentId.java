package com.mongodb.hvdf.oid;

import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hvdf.api.Sample;

public class SourceTimeDocumentId implements SampleId {

	private long time = 0;
	private Object source = null;
	private DBObject oid = null;
	
	public SourceTimeDocumentId(Object source, long timeStamp) {
		time = timeStamp;
	}

	public SourceTimeDocumentId(Object docId) {
		oid = (DBObject)docId;
	}

	@Override
	public boolean embedsTime() {
		return true;
	}

	@Override
	public boolean embedsSource() {
		return true;
	}

	@Override
	public long getTime() {
		
		// If the time is not known and the oid exists
		// extract the time from the oid
		if(time == 0 && oid != null){
			time = ((Long)oid.get(Sample.TS_KEY));	
		}
		
		return time;
	}

	@Override
	public Object getSourceId() {
		if(source == null && oid != null){
			source = oid.get(Sample.SOURCE_KEY);
		}
		
		return source;
	}

	@Override
	public Object toObject() {
		
		if(oid == null){
			oid = new BasicDBObject(Sample.SOURCE_KEY, this.source).append(Sample.TS_KEY, time);
		}
		
		return oid;
	}
	

}
