package com.mongodb.hvdf.services;

import com.mongodb.DBObject;
import com.mongodb.hvdf.channels.Channel;
import com.mongodb.hvdf.util.JSONParam;

public interface ChannelService extends Service{
	
	public Channel getChannel(String feedName, String channelName);

	public void configureChannel(String feedName, String channelName, JSONParam config);

	public DBObject getChannelConfiguration(String feedName, String channelName);
	
}
