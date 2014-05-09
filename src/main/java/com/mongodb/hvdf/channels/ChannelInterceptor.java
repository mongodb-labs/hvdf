package com.mongodb.hvdf.channels;

import com.mongodb.DBObject;
import com.mongodb.hvdf.configuration.PluginConfiguration;

public abstract class ChannelInterceptor implements ChannelProcessor{
	
	protected ChannelProcessor next;
	protected PluginConfiguration config;
	
	@Override
	public void shutdown(){}
	
	@Override
	public void configure(DBObject configuration){
		this.config = new PluginConfiguration(configuration, this.getClass());
	}
	
	protected void setNext(ChannelProcessor next){
		this.next = next;
	}	
}
