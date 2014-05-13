package com.mongodb.hvdf.channels;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;

import org.bson.types.ObjectId;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.hvdf.allocators.CollectionAllocator;
import com.mongodb.hvdf.allocators.SingleCollectionAllocator;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.interceptors.RequiredFieldsInterceptor;
import com.mongodb.hvdf.oid.HiDefTimeIdFactory;
import com.mongodb.hvdf.oid.SampleIdFactory;
import com.mongodb.hvdf.util.MongoDBQueryHelpers;

public class SimpleChannel implements Channel {

    private static Logger logger = LoggerFactory.getLogger(SimpleChannel.class);

	// Config items and defaults	
	private static final String TIME_SLICING_KEY = "time_slicing";
	private static final PluginConfiguration DEFAULT_TIME_SLICING = 
			new PluginConfiguration(new BasicDBObject(PluginFactory.TYPE_KEY, 
					SingleCollectionAllocator.class.getName()), SimpleChannel.class);
	private static final String INTERCEPTOR_LIST = "interceptors";
	private static final String LISTENER_LIST = "listeners";
	
	// Channel components
	private SampleIdFactory idFactory;
	private CollectionAllocator allocator;
	private ChannelInterceptor interceptorChain;
	private List<ChannelListener> listeners;
	private ChannelInterceptor loopbackInterceptor = new ChannelInterceptor() {

		@Override
		public void pushSample(DBObject sample, boolean isList, BasicDBList resultList) {

			storeSample(sample, isList, resultList);
		}

	};
	
	// Database reference and prefix for collection names
	private final DB database;
	private final String channelName;

	private static final BasicDBObject idDescending = 
			new BasicDBObject(Sample.ID_KEY, -1);
	
	SimpleChannel(DB db, String channelName){
		this.database = db;
		this.channelName = channelName;		
		configure(null);
	}
	
	@Override
	public void pushSample(DBObject sample, boolean isList, BasicDBList resultList) {
		
		// Perform pre-processing through the interceptors
		interceptorChain.pushSample(sample, isList, resultList);
		
		// TODO: Post process all channel listeners				
	}

	@Override
	public List<Sample> query(long timeStart, long timeRange,
			DBObject query, DBObject projection, int limit) {
		
		// Get the initial collection from which to query
		DBCollection current = allocator.getCollection(timeStart);
		
		// Setup the query for custom terms and time range
		long minTime = timeStart - timeRange;
		
		BasicDBObject fullQuery = new BasicDBObject();
		if(query != null) fullQuery.putAll(query);
		fullQuery.append(Sample.ID_KEY, idFactory.limitIdTimeRange(0, timeStart, minTime));
		
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
	public void configure(DBObject configuration) {
		
		// Some configuration context for plugins
		HashMap<String, Object> injectedConfig = new HashMap<String, Object>();
		injectedConfig.put("database", database);
		injectedConfig.put("prefix", channelName + "_raw_");
		
		// Use the PluginConfiguration class to represent the raw config
		PluginConfiguration parsedConfig = new PluginConfiguration(configuration, this.getClass());
		
		// Create the timeslice allocator
		PluginConfiguration allocatorConfig = parsedConfig.get(
				TIME_SLICING_KEY, PluginConfiguration.class, DEFAULT_TIME_SLICING);
		allocator = PluginFactory.loadPlugin(CollectionAllocator.class, allocatorConfig, injectedConfig);	
		
		// We always have a loopback interceptor for storing the actual document
		this.interceptorChain = this.loopbackInterceptor ;
		
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
		
		// Use an ObjectId that can support ms resolution time and
		// install the interceptor to insert them
		this.idFactory = new HiDefTimeIdFactory();

		// Required fields interceptor goes on the front
		RequiredFieldsInterceptor requiredFields = 
				new RequiredFieldsInterceptor(null, this.idFactory);
		requiredFields.setNext(this.interceptorChain);
		this.interceptorChain = requiredFields;
		
		// Create an empty list of listeners
		this.listeners = new ArrayList<ChannelListener>();
	}

	private void storeSample(DBObject sample, boolean isList, BasicDBList resultList) {
				
		if(isList){
			
			// Use the batch API to send a number of samples
			storeBatch((BasicDBList)sample);			
		}
		else if(sample != null){
			
			// This is a document, place it stright in appropriate collection
			BasicDBObject doc = ((BasicDBObject) sample);
			long timestamp = doc.getLong(Sample.TS_KEY);
			DBCollection collection = allocator.getCollection(timestamp);
			collection.insert(doc);
		}
	}

	private void storeBatch(BasicDBList sample) {
		
		// The batch may span collection splits, so maintain
		// a current collection and batch operation
		BulkWriteOperation currentOp = null;
		DBCollection currentColl = null;
		
		logger.debug("Received batch of size : {}", sample.size());
		for(Object sampleObj : sample){
			
			// prepare the sample to batch
			BasicDBObject doc = ((BasicDBObject) sampleObj);
			long timestamp = doc.getLong(Sample.TS_KEY);
			DBCollection collection = allocator.getCollection(timestamp);
			
			// if the collection has changed, commit the current
			// batch to the collection and start new
			if(collection.equals(currentColl) == false){
				executeBatchWrite(currentOp, sample);
				currentColl = collection;
				currentOp = collection.initializeUnorderedBulkOperation();
			}
			
			// put the doc insert into the batch
			currentOp.insert(doc);
		}		
		
		// Finalize the last batch
		executeBatchWrite(currentOp, sample);			
	}


	private void executeBatchWrite(BulkWriteOperation batchOp, BasicDBList fullBatch) {

		if(batchOp != null){
			try{
				BulkWriteResult result = batchOp.execute();
				logger.debug("Wrote sample batch - sent {} : inserted {}", 
						fullBatch.size(), result.getInsertedCount());
			} catch(BulkWriteException bwex){
				
				// TODO : collect and log information about failure, pull
				// out the relevant docs from fullBatch and construct a 
				// ServiceException that has all the information
			}
		}
	}

	@Override
	public void shutdown() {
		
	}
}
