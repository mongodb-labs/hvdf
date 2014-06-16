package com.mongodb.hvdf.interceptors;


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
import com.mongodb.hvdf.channels.ChannelInterceptor;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.configuration.PluginConfiguration.HVDF;
import com.mongodb.hvdf.oid.SampleId;
import com.mongodb.hvdf.oid.SampleIdFactory;

public class RawStorageInterceptor extends ChannelInterceptor{

    private static Logger logger = LoggerFactory.getLogger(RawStorageInterceptor.class);

	private final CollectionAllocator collectionAllocator;
	private final SampleIdFactory idFactory;
	

	public RawStorageInterceptor(PluginConfiguration config){
		
		// Need an allocator to work on collections and ids
		this.collectionAllocator = config.get(HVDF.ALLOCATOR, CollectionAllocator.class);
		this.idFactory = config.get(HVDF.ID_FACTORY, SampleIdFactory.class);
	}
		
	public void pushSample(DBObject sample, boolean isList, BasicDBList resultList) {
		
		if(isList){
			
			// Use the batch API to send a number of samples
			storeBatch((BasicDBList)sample, resultList);			
		}
		else if(sample != null){
			
			// Create an oid to embed the sample time
			BasicDBObject doc = ((BasicDBObject) sample);
			SampleId _id = idFactory.createId(sample);
			sample.put(Sample.ID_KEY, _id.toObject());
			resultList.add(_id.toObject());

			// Get the correct slice from the allocator and insert
			long timestamp = doc.getLong(Sample.TS_KEY);
			DBCollection collection = collectionAllocator.getCollection(timestamp);
			collection.insert(doc);
		}
	}

	private void storeBatch(BasicDBList sample, BasicDBList resultList) {
		
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
				SampleId _id = idFactory.createId(doc);
				doc.put(Sample.ID_KEY, _id.toObject());
				resultList.add(_id.toObject());
				long timestamp = doc.getLong(Sample.TS_KEY);
				DBCollection collection = collectionAllocator.getCollection(timestamp);
				
				// if the collection has changed, commit the current
				// batch to the collection and start new
				if(collection.equals(currentColl) == false){
					executeBatchWrite(currentOp, sample);
					currentColl = collection;
					currentOp = collection.initializeUnorderedBulkOperation();
					currentOpOffset = sampleIdx;
				}
				
				// put the doc insert into the batch
				currentOp.insert(doc);
			}		
			
			// Finalize the last batch
			executeBatchWrite(currentOp, sample);		
			
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
			
			// TODO : we also need to handle the result Ids here as well,
			// the failed doc Ids must be pulled from the resultList
			throw ex;
		}
	}


	private void executeBatchWrite(BulkWriteOperation batchOp, BasicDBList fullBatch) {

		if(batchOp != null){
			BulkWriteResult result = batchOp.execute();
			logger.debug("Wrote sample batch - sent {} : inserted {}", 
					fullBatch.size(), result.getInsertedCount());
		}
	}
}
