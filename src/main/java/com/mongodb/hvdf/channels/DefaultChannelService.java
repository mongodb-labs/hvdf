package com.mongodb.hvdf.channels;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantReadWriteLock;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hvdf.MongoBackedService;
import com.mongodb.hvdf.configuration.ChannelServiceConfiguration;
import com.mongodb.hvdf.services.ChannelService;
import com.mongodb.hvdf.services.ServiceImplementation;
import com.mongodb.hvdf.util.JSONParam;
import com.yammer.dropwizard.config.Configuration;

@ServiceImplementation(
        name = "DefaultChannelService", 
        dependencies = { },
        configClass = ChannelServiceConfiguration.class)
public class DefaultChannelService extends MongoBackedService
	implements ChannelService{
	
	
	private static final String CHANNEL_CONFIG = "hvdf_channels";

	private final ReentrantReadWriteLock channelLock = 
			new ReentrantReadWriteLock();
	
	private final Map<String, Channel> channelMap = 
			new HashMap<String, Channel>();

	private final ChannelServiceConfiguration config;

	

	public DefaultChannelService(
			final MongoClientURI dbUri, 
			final ChannelServiceConfiguration config){
		super(dbUri, config);
		
		this.config = config;
	}
	
	@Override
	public Channel getChannel(String feedName, String channelName) {
		
		// Take the read lock and see if the channel exists
		Channel channel = null;
		String fullName = feedName + "-" + channelName;
		channelLock.readLock().lock();
		try{
			channel = channelMap.get(fullName);
		}
		finally{
			channelLock.readLock().unlock();
		}
		
		if(channel == null){
			DBObject channelConfig = getChannelConfiguration(feedName, channelName);
			channelLock.writeLock().lock();
			try{
				// make sure it wasnt created between locks
				channel = channelMap.get(fullName);
				
				if(channel == null){
					// if still isn't there, build it
					DB database = getFeedDatabase(feedName);
					channel = new SimpleChannel(database, channelName);					
					channel.configure(channelConfig);
					channelMap.put(fullName, channel);
				}
			}
			finally{
				channelLock.writeLock().unlock();
			}
		}
		return channel;
	}

	private DB getFeedDatabase(String feedName) {
		
		// screech will need to manage having separate servers 
		// configurable for individual feeds, for now it uses
		// the server configured for all of them.
		return this.client.getDB(feedName);
	}

	private DBCollection getChannelConfigCollection(String feedName) {
		
		DB feedDb = this.client.getDB(feedName);
		DBCollection channelColl = feedDb.getCollection(CHANNEL_CONFIG);	
		return channelColl;
	}

	@Override
	public DBObject getChannelConfiguration(String feedName, String channelName) {
		
		DB feedDb = this.client.getDB(feedName);
		DBCollection channelColl = feedDb.getCollection(CHANNEL_CONFIG);	
		return channelColl.findOne(new BasicDBObject("_id", channelName));
	}

	@Override
	public void configureChannel(String feedName, String channelName,
			JSONParam config) {
		
		// Now store the channel config persistently
		DBCollection configColl = getChannelConfigCollection(feedName);
		BasicDBObject newConfig = new BasicDBObject();
		newConfig.putAll(config.toDBObject());
		newConfig.put("_id", channelName);
		configColl.update(new BasicDBObject("_id", channelName), newConfig, true, false);	
	}


	@Override
	public void shutdown(long timeout, TimeUnit unit) {
		// TODO: Flush all channels to ensure any batching etc completes		
	}

	@Override
	public Configuration getConfiguration() {
		return this.config;
	}

}
