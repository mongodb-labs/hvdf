package com.mongodb.hvdf.tasks;

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.hvdf.allocators.CollectionAllocator;
import com.mongodb.hvdf.allocators.SliceDetails;
import com.mongodb.hvdf.channels.ChannelTask;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.configuration.PluginConfiguration.HVDF;

public class IndexingTask extends ChannelTask {
	
    private static Logger logger = LoggerFactory.getLogger(IndexingTask.class);
	
	private final CollectionAllocator allocator;
	private final List<PluginConfiguration> indexList; 
	private final DB db;
	
	
	private Map<String, IndexRecord> indexCache = new HashMap<String, IndexRecord>();
	
	public IndexingTask(PluginConfiguration config){
		
		this.allocator = config.get(HVDF.ALLOCATOR, CollectionAllocator.class);
		this.db = config.get(HVDF.DB, DB.class);
		this.indexList = config.getList("indexes", PluginConfiguration.class);		
		
		// validate that all index entries at least have keys
		for(PluginConfiguration indexConfig : indexList){
			indexConfig.get("keys", DBObject.class);
		}
		
	}

	@Override
	public void run() {
		
		// Get a sorted list of slices 
		List<SliceDetails> slices = allocator.getCollectionSlices();
		Collections.sort(slices, SliceDetails.Comparators.MIN_TIME_DESCENDING);
		HashSet<String> currentCacheKeys = new HashSet<String>(this.indexCache.keySet());
		
		// For each collection look for the namespace in the cache	
		int order = 0;
		for(SliceDetails slice : slices){
			
			// Find the cache entry for this collection or create it
			currentCacheKeys.remove(slice.name);
			IndexRecord nsCache = this.indexCache.get(slice.name);
			if(nsCache == null){
				nsCache = new IndexRecord();
				this.indexCache.put(slice.name, nsCache);
			}
			
			// process if the cache is not complete for all configured keys
			if(nsCache.complete == false){
				
				nsCache.complete = true;
				for(PluginConfiguration indexConfig : indexList){
					
					// Index may be configured to not apply to n most recent
					int skips = indexConfig.get("skips", Integer.class, 0);
					if(order >= skips){
						
						try{
							// Now check if the index was already added
							DBObject proposed = indexConfig.get("keys", DBObject.class);
							if(nsCache.keySet.contains(proposed) == false){
								
								// Add the proposed index per config
								DBCollection coll = this.db.getCollection(slice.name);
								DBObject options = indexConfig.get("options", DBObject.class, new BasicDBObject());
								coll.createIndex(proposed, options);
								nsCache.keySet.add(proposed);
							}
						} catch(Exception ex){
							nsCache.complete = false;
							logger.warn("Exception in channel task", ex);
						}
						
					} else {						
						// Since this was skipped, mark as incomplete
						nsCache.complete = false;
					}					
				}
			}
			
			order++;
		}
		
		// The remainder of the keys in currentCacheKeys no longer exist
		// so they can be removed from the local cache also
		for(String togo : currentCacheKeys){
			this.indexCache.remove(togo);
		}
		
	}	
	
	// Record used to track which indexes have already been added
	class IndexRecord {
		
		public HashSet<DBObject> keySet = new HashSet<DBObject>();
		public boolean complete = false;		
	}
}
	
	
