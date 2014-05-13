package com.mongodb.hvdf.channels;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;

public interface ChannelProcessor {

	public void pushSample(DBObject sample, boolean isList, BasicDBList resultIds);

}
