package com.mongodb.hvdf.interceptors;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.BulkWriteError;
import com.mongodb.BulkWriteException;
import com.mongodb.BulkWriteOperation;
import com.mongodb.BulkWriteResult;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.hvdf.allocators.CollectionAllocator;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.channels.PluginFactory;
import com.mongodb.hvdf.channels.StorageInterceptor;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.configuration.PluginConfiguration.HVDF;
import com.mongodb.hvdf.configuration.TimePeriod;
import com.mongodb.hvdf.oid.SourceTimeDocumentIdFactory;
import com.mongodb.hvdf.rollup.RollupOperation;

public class RollupStorageInterceptor extends StorageInterceptor{

	private static Logger logger = LoggerFactory.getLogger(RollupStorageInterceptor.class);

    private static final String ROLLUP_OPS_CONFIG = "rollup_ops";
    private static final String ROLLUP_PERIOD_CONFIG = "document_period";
	
	private final CollectionAllocator collectionAllocator;
	private final List<RollupOperation> rollupOps;
	private final long rollupPeriod;
	

	public RollupStorageInterceptor(PluginConfiguration config){
		
		super(config);
		
		// Need an allocator to work on collections and ids
		this.collectionAllocator = config.get(HVDF.ALLOCATOR, CollectionAllocator.class);
		
		this.rollupPeriod = (config.get(ROLLUP_PERIOD_CONFIG, TimePeriod.class)).getAs(TimeUnit.MILLISECONDS);
		
		// Get the rollup operations configuration for the channel
		List<PluginConfiguration> opsConfig = config.getList(ROLLUP_OPS_CONFIG, PluginConfiguration.class);
		rollupOps = new ArrayList<RollupOperation>(opsConfig.size());
		
		// Load each rollup operation requested
		for(PluginConfiguration opConfig : opsConfig){
			rollupOps.add(PluginFactory.loadPlugin(RollupOperation.class, opConfig));
		}
	}
		
    @Override
	protected PluginConfiguration getDefaultIdFactoryConfig() {
    	
    	// By default, the rollup channel uses a source-time doc as ID
		return new PluginConfiguration(new BasicDBObject(PluginFactory.TYPE_KEY, 
				SourceTimeDocumentIdFactory.class.getName()), StorageInterceptor.class);
	}

	@Override
	public void pushSample(DBObject sample, boolean isList, BasicDBList resultIds) {
		
		if(isList){
			
			// Use the batch API to send a number of samples
			updateBatch((BasicDBList)sample);			
		}
		else if(sample != null){
			
			// This is a document, place it straight in appropriate collection
			BasicDBObject doc = ((BasicDBObject) sample);
			long timestamp = this.rollupPeriod * (doc.getLong(Sample.TS_KEY) / this.rollupPeriod);			
			DBCollection collection = collectionAllocator.getCollection(timestamp);
			
			// Ask the id allocator for the query
			BasicDBObject query = this.idFactory.getQuery(sample.get(Sample.SOURCE_KEY), timestamp);
			
			// Build the update clause using the ops list
			BasicDBObject update = new BasicDBObject();
			for(RollupOperation rollupOp : this.rollupOps){
				
				DBObject updateClause = rollupOp.getUpdateClause(sample);
				
				// Check for top level operators that already exist so they dont overwrite
				for(String key : updateClause.keySet()){
					BasicDBObject existingClause = (BasicDBObject) update.get(key);
					if(existingClause != null){
						// Merge the arguments to the top level op
						existingClause.putAll((DBObject)updateClause.get(key));
					} else {
						update.put(key, updateClause.get(key));
					}
				}
			}
			
			collection.update(query, update, true, false);
		}
	}	
	
	private void updateBatch(BasicDBList sample) {
		
		// The batch may span collection splits, so maintain
		// a current collection and batch operation
		BulkWriteOperation currentOp = null;
		int currentOpOffset = 0;
		int sampleIdx = 0;
		DBCollection currentColl = null;	
		
		logger.debug("Received batch of size : {}", sample.size());
		
		try{
			for(; sampleIdx < sample.size(); ++sampleIdx){
				
				// prepare the sample to batch
				BasicDBObject doc = (BasicDBObject) (sample.get(sampleIdx));
				long timestamp = this.rollupPeriod * (doc.getLong(Sample.TS_KEY) / this.rollupPeriod);			
				DBCollection collection = collectionAllocator.getCollection(timestamp);
				
				// if the collection has changed, commit the current
				// batch to the collection and start new
				if(collection.equals(currentColl) == false){
					executeBatchUpdate(currentOp, sample);
					currentColl = collection;
					currentOp = collection.initializeUnorderedBulkOperation();
					currentOpOffset = sampleIdx;
				}
				
				// put the doc insert into the batch
				// Ask the id allocator for the query
				BasicDBObject query = this.idFactory.getQuery(doc.get(Sample.SOURCE_KEY), timestamp);
				
				// Build the update clause using the ops list
				BasicDBObject update = new BasicDBObject();
				for(RollupOperation rollupOp : this.rollupOps){
					
					DBObject updateClause = rollupOp.getUpdateClause(doc);
					
					// Check for top level operators that already exist so they dont overwrite
					for(String key : updateClause.keySet()){
						BasicDBObject existingClause = (BasicDBObject) update.get(key);
						if(existingClause != null){
							// Merge the arguments to the top level op
							existingClause.putAll((DBObject)updateClause.get(key));
						} else {
							update.put(key, updateClause.get(key));
						}
					}
				}
				
				currentOp.find(query).upsert().updateOne(update);
			}		
			
			// Finalize the last batch
			executeBatchUpdate(currentOp, sample);		
			
		} catch(Exception ex){
			
			// One of the bulk writes has failed
			BasicDBList failedDocs = new BasicDBList();
			if(ex instanceof BulkWriteException){
				
				// We need to figure out the failures and remove the writes
				// that worked from the batch
				int batchSize = sampleIdx - currentOpOffset;
				BulkWriteException bwex = (BulkWriteException)ex;
				int errorCount = bwex.getWriteErrors().size(); 
				if(errorCount < batchSize){
					
					for(BulkWriteError we : bwex.getWriteErrors()){
						failedDocs.add(sample.get(currentOpOffset + we.getIndex()));
					}
					
					// since we have accounted for the failures in the current
					// batch, move the offset forward to the last sample
					currentOpOffset = sampleIdx;					
				}
			}
			
			// If this happened part way through the batch, send remaining 
			// docs to failed list and update sample to contain only failed docs
			if(currentOpOffset > 0){
				for(; currentOpOffset < sample.size(); ++currentOpOffset)
					failedDocs.add(sample.get(currentOpOffset));
				sample.clear();
				sample.addAll(failedDocs);	
			}
			
			throw ex;
		}
	}


	private void executeBatchUpdate(BulkWriteOperation batchOp, BasicDBList fullBatch) {

		if(batchOp != null){
			BulkWriteResult result = batchOp.execute();
			logger.debug("Wrote sample batch - sent {} : updated {}", 
					fullBatch.size(), result.getModifiedCount());
		}
	}
	
}
