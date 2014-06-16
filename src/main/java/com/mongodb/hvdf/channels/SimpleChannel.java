package com.mongodb.hvdf.channels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.hvdf.allocators.CollectionAllocator;
import com.mongodb.hvdf.allocators.SingleCollectionAllocator;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.configuration.PluginConfiguration.HVDF;
import com.mongodb.hvdf.configuration.TimePeriod;
import com.mongodb.hvdf.interceptors.RawStorageInterceptor;
import com.mongodb.hvdf.interceptors.RequiredFieldsInterceptor;
import com.mongodb.hvdf.oid.HiDefTimeIdFactory;
import com.mongodb.hvdf.oid.SampleIdFactory;
import com.mongodb.hvdf.util.MongoDBQueryHelpers;

public class SimpleChannel implements Channel {

    private static Logger logger = LoggerFactory.getLogger(SimpleChannel.class);

	// Config items and defaults	
	private static final String STORAGE_KEY = "storage";
	private static final String TIME_SLICING_KEY = "time_slicing";
	private static final String ID_FACTORY_KEY = "id_type";
	private static final PluginConfiguration DEFAULT_ID_FACTORY = 
			new PluginConfiguration(new BasicDBObject(PluginFactory.TYPE_KEY, 
					HiDefTimeIdFactory.class.getName()), SimpleChannel.class);
	private static final PluginConfiguration DEFAULT_TIME_SLICING = 
			new PluginConfiguration(new BasicDBObject(PluginFactory.TYPE_KEY, 
					SingleCollectionAllocator.class.getName()), SimpleChannel.class);
	private static final PluginConfiguration DEFAULT_STORAGE = 
			new PluginConfiguration(new BasicDBObject(PluginFactory.TYPE_KEY, 
					RawStorageInterceptor.class.getName()), SimpleChannel.class);
	private static final String INTERCEPTOR_LIST = "interceptors";
	private static final String LISTENER_LIST = "listeners";
	private static final String TASK_LIST = "tasks";
	
	// Channel components
	private SampleIdFactory idFactory;
	private CollectionAllocator allocator;
	private ChannelInterceptor interceptorChain;
	private List<ChannelListener> listeners;
	private List<ChannelTask> taskList;
	
	// Database reference and prefix for collection names
	private final DB database;
	private final String channelName;
	private final String feedName;

	private static final BasicDBObject idDescending = 
			new BasicDBObject(Sample.ID_KEY, -1);
		
	SimpleChannel(DB db, String feedName, String channelName){
		this.database = db;
		this.feedName = feedName;
		this.channelName = channelName;		
	}
	
	@Override
	public void pushSample(DBObject sample, boolean isList, BasicDBList resultList) {
		
		// Perform pre-processing through the interceptors
		interceptorChain.pushSample(sample, isList, resultList);
		
		// TODO: Post process all channel listeners				
	}

	@Override
	public List<Sample> query(Object source, long timeStart, long timeRange,
			DBObject query, DBObject projection, int limit) {
		
		// Get the initial collection from which to query
		DBCollection current = allocator.getCollection(timeStart);
		
		// Setup the query for custom terms and time range
		long minTime = timeStart - timeRange;
		
		BasicDBObject fullQuery = new BasicDBObject();
		if(query != null) fullQuery.putAll(query);
		
		BasicDBObject sourceTimeClause = idFactory.getTimeRangeQuery(source, timeStart, minTime);
		fullQuery.putAll((DBObject)sourceTimeClause);
		
		// Get the initial result batch from the first collection
		List<Sample> result = new ArrayList<Sample>();
		
		while(current != null && result.size() < limit){			
			
			// Query the current collection and pull as much as possible
			int howManyMore = limit - result.size();
			DBCursor cursor = current.find(fullQuery, projection).sort(idDescending);
			MongoDBQueryHelpers.getSamplesFromCursor(cursor, howManyMore, result);

			// Get the previous collection (in time)
			current = allocator.getPreviousWithLimit(current, minTime);
		}
				
		return result;
	}

    
	@Override
	public void removeSample(Object sampleId) {
		ObjectId oid = (ObjectId)sampleId;
		com.mongodb.hvdf.oid.SampleId _id = 
			idFactory.createId(oid);
		DBCollection collection = allocator.getCollection(_id.getTime());
		collection.remove(new BasicDBObject(Sample.ID_KEY, oid));
	}

	@Override
	public void configure(DBObject configuration, ChannelTaskScheduler scheduler) {
		
		// Some configuration context for plugins
		HashMap<String, Object> injectedConfig = new HashMap<String, Object>();
		injectedConfig.put(HVDF.DB, database);
		injectedConfig.put(HVDF.PREFIX, channelName + "_raw_");
		
		// Use the PluginConfiguration class to represent the raw config
		PluginConfiguration parsedConfig = new PluginConfiguration(configuration, this.getClass());
		
		// Create the timeslice allocator
		PluginConfiguration allocatorConfig = parsedConfig.get(
				TIME_SLICING_KEY, PluginConfiguration.class, DEFAULT_TIME_SLICING);
		allocator = PluginFactory.loadPlugin(CollectionAllocator.class, allocatorConfig, injectedConfig);
		injectedConfig.put(HVDF.ALLOCATOR, this.allocator);
		
		// Use an ObjectId that can support ms resolution time and
		// install the interceptor to insert them
		PluginConfiguration idFactoryConfig = parsedConfig.get(
				ID_FACTORY_KEY, PluginConfiguration.class, DEFAULT_ID_FACTORY);
		idFactory = PluginFactory.loadPlugin(SampleIdFactory.class, idFactoryConfig, injectedConfig);
		injectedConfig.put(HVDF.ID_FACTORY, this.idFactory);

		// Setup the interceptor chain
		configureInterceptors(parsedConfig, injectedConfig);
				
		// Create an empty list of listeners
		this.listeners = new ArrayList<ChannelListener>();

		// Setup tasks for the channel 
		configureTasks(parsedConfig, scheduler, injectedConfig);
	}

	private void configureTasks(PluginConfiguration parsedConfig, 
			ChannelTaskScheduler scheduler, HashMap<String, Object> injectedConfig) {
		
		scheduler.cancelAllForChannel(this.feedName, this.channelName);
		
		// Load the task list if present		
		List<PluginConfiguration> configList = parsedConfig.getList(TASK_LIST, PluginConfiguration.class);
		this.taskList = new ArrayList<ChannelTask>(configList.size());

		// Get each configured task object
		for(PluginConfiguration pluginConfig : configList){			
			long period = (pluginConfig.get(ChannelTask.PERIOD_KEY, TimePeriod.class)).getAs(TimeUnit.MILLISECONDS);
			ChannelTask current = PluginFactory.loadPlugin(ChannelTask.class, pluginConfig, injectedConfig);
			if(current != null){				
				current.setPeriod(period);
				this.taskList.add(current);
				scheduler.scheduleTask(this.feedName, this.channelName, current, period);
			}
		}		
	}

	private void configureInterceptors(PluginConfiguration parsedConfig, HashMap<String, Object> injectedConfig) {
		
		// We always have a terminating storage interceptor for storing the actual document
		PluginConfiguration storageConfig = parsedConfig.get(
				STORAGE_KEY, PluginConfiguration.class, DEFAULT_STORAGE);
		this.interceptorChain = PluginFactory.loadPlugin(ChannelInterceptor.class, storageConfig, injectedConfig);
		
		// Load the interceptor list if present		
		List<PluginConfiguration> configList = parsedConfig.getList(INTERCEPTOR_LIST, PluginConfiguration.class);

		// Get the interceptor for each array element (last first)
		Collections.reverse(configList);
		for(PluginConfiguration pluginConfig : configList){				
			ChannelInterceptor current = PluginFactory.loadPlugin(ChannelInterceptor.class, pluginConfig);
			if(current != null){				
				// insert the current interceptor into the chain
				current.setNext(this.interceptorChain);
				this.interceptorChain = current;
			}
		}
		
		// Required fields interceptor goes on the front
		RequiredFieldsInterceptor requiredFields = new RequiredFieldsInterceptor(null);
		requiredFields.setNext(this.interceptorChain);
		this.interceptorChain = requiredFields;		
	}

	@Override
	public void shutdown() {
		
	}
}
