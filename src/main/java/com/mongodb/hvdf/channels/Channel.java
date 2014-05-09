package com.mongodb.hvdf.channels;

import java.util.List;

import com.mongodb.DBObject;
import com.mongodb.hvdf.api.Sample;

public interface Channel extends ChannelProcessor{

	public List<Sample> query(long timeStart, long timeRange,
			DBObject query, DBObject projection, int limit);
	
	public void removeSample(Object sampleId);
}
