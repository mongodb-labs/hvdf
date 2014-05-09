package com.mongodb.hvdf.channels;

import com.mongodb.DBObject;
import com.mongodb.hvdf.api.Source;

public interface ChannelListener {
	
	public void pushSample(Source source, long timeStamp, DBObject sample);

	public void configure(DBObject configuration);

}
