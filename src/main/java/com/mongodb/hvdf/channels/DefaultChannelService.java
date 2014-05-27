package com.mongodb.hvdf.channels;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
import com.mongodb.hvdf.util.MongoDBCommands;
import com.yammer.dropwizard.config.Configuration;

@ServiceImplementation(
        name = "DefaultChannelService", 
        dependencies = { },
        configClass = ChannelServiceConfiguration.class)
public class DefaultChannelService extends MongoBackedService
	implements ChannelService, ChannelTaskScheduler{
	
	
	private static final String CHANNEL_CONFIG = "hvdf_channels_";

	private final ReentrantReadWriteLock channelLock = 
			new ReentrantReadWriteLock();
	
	private final Map<String, Channel> channelMap = 
			new HashMap<String, Channel>();

	private final Map<String, Map<String, List<ScheduledFuture<?>>>> tasks = 
			new HashMap<String, Map<String, List<ScheduledFuture<?>>>>();

	private final ScheduledExecutorService taskExecutor; 

	private final ChannelServiceConfiguration config;

	
	public DefaultChannelService(
			final MongoClientURI dbUri, 
			final ChannelServiceConfiguration config){
		super(dbUri, config);
		
		this.config = config;
		
		if(this.config.channel_task_thread_pool_size > 0){
			this.taskExecutor = Executors.newScheduledThreadPool(
					this.config.channel_task_thread_pool_size);
		}
		else {
			this.taskExecutor = null;
		}
		
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
					channel = new SimpleChannel(database, feedName, channelName);					
					channel.configure(channelConfig, this);
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
		
		DB configDb = this.client.getDB("config");
		return configDb.getCollection(CHANNEL_CONFIG + feedName);	
	}

	@Override
	public DBObject getChannelConfiguration(String feedName, String channelName) {
		
		DBCollection configColl = getChannelConfigCollection(feedName);
		return configColl.findOne(new BasicDBObject("_id", channelName));
	}

	@Override
	public void configureChannel(String feedName, String channelName,
			JSONParam channelConfig) {
		
		// Now store the channel config persistently
		DBCollection configColl = getChannelConfigCollection(feedName);
		BasicDBObject newConfig = new BasicDBObject();
		newConfig.putAll(channelConfig.toDBObject());
		newConfig.put("_id", channelName);
		
		// NOTE : Switched to using a command for this update since we want
		// to store index configuration natively, but they often contain periods
		// in their field names. Using a command avoids the client side checking
		// which prohibits this and allows use of the "config" db on the server.
		// 
		// configColl.update(new BasicDBObject("_id", channelName), newConfig, true, false);
		
		MongoDBCommands.update(configColl, new BasicDBObject("_id", channelName), newConfig, true, false);	
	}


	@Override
	public void shutdown(long timeout, TimeUnit unit) {
		// If this instance is running tasks, then schedule it
		if(this.taskExecutor != null){
			this.taskExecutor.shutdown();
		}

		// TODO: Flush all channels to ensure any batching etc completes
		
		// call to the channel service 
		super.shutdown(timeout, unit);;	
	}

	@Override
	public Configuration getConfiguration() {
		return this.config;
	}

	@Override
	public ScheduledFuture<?> scheduleTask(
			String feedName, String channelName,
			ChannelTask task, long msPeriod) {
		
		// If this instance is running tasks, then schedule it
		if(this.taskExecutor != null){
			
			List<ScheduledFuture<?>> channelList = getChannelTaskList(feedName, channelName);		
			ScheduledFuture<?> handle = this.taskExecutor.scheduleWithFixedDelay(
					task, msPeriod, msPeriod, TimeUnit.MILLISECONDS);
			channelList.add(handle);
			return handle;
		}

		return null;
	}

	@Override
	public void cancelAllForChannel(String feedName, String channelName) {

		List<ScheduledFuture<?>> channelList = getChannelTaskList(feedName, channelName);
		for(ScheduledFuture<?> toCancel : channelList){
			toCancel.cancel(false);
		}
	}
	
	private List<ScheduledFuture<?>> getChannelTaskList(String feedName, String channelName){
		
		Map<String, List<ScheduledFuture<?>>> feedList = this.tasks.get(feedName);
		if(feedList == null){
			feedList = new HashMap<String, List<ScheduledFuture<?>>>();
			this.tasks.put(feedName, feedList);
		}

		List<ScheduledFuture<?>> channelList = feedList.get(channelName);
		if(channelList == null){
			channelList = new ArrayList<ScheduledFuture<?>>();
			feedList.put(channelName, channelList);
		}
		
		return channelList;
	}

}
