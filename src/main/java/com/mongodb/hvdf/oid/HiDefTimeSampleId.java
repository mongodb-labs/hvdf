package com.mongodb.hvdf.oid;

import org.bson.types.ObjectId;

public class HiDefTimeSampleId implements SampleId {

	private long time = 0;
	private ObjectId oid = null;
	
	public HiDefTimeSampleId(long timeStamp) {
		time = timeStamp;
	}

	public HiDefTimeSampleId(ObjectId docId) {
		oid = docId;
	}

	@Override
	public boolean embedsTime() {
		return true;
	}

	@Override
	public boolean embedsSource() {
		return false;
	}

	@Override
	public long getTime() {
		
		// If the time is not known and the oid exists
		// extract the time from the oid
		if(time == 0 && oid != null){
			time = readTimeStamp(oid.toByteArray());	
		}
		
		return time;
	}

	@Override
	public long getSourceId() {
		return 0;
	}

	@Override
	public ObjectId toObjectId() {
		
		if(oid == null){
			
			// Create a regular ObjectId and get the bytes
			byte[] oidBytes = (new ObjectId()).toByteArray();
			
			// Paste the 6 byte HD time and construct OID
			writeTimeStamp(oidBytes, time);
			oid = new ObjectId(oidBytes);
		}
		
		return oid;
	}
	
	protected static void writeTimeStamp(byte[] targetBytes, long timeStamp){
		// Copy 6 timestamp bytes into the OID bytes
		for(int shift = 40; shift >= 0; shift -= 8 ){
			targetBytes[(40 - shift)/8] = (byte)(timeStamp >>> shift);
		}		
	}

	protected static long readTimeStamp(byte[] targetBytes){
		
		long ts = 0;
		
		// Copy 6 timestamp bytes from the OID bytes
		for(int shift = 0; shift <= 40; shift += 8 ){
			ts = (ts << shift) | targetBytes[(40 - shift)/8];
		}		
		
		return ts;
	}

}
