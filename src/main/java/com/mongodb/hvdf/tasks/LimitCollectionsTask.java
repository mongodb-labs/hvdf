package com.mongodb.hvdf.tasks;

import java.util.Collections;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.DB;
import com.mongodb.hvdf.allocators.CollectionAllocator;
import com.mongodb.hvdf.allocators.SliceDetails;
import com.mongodb.hvdf.channels.ChannelTask;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.configuration.PluginConfiguration.HVDF;
import com.mongodb.hvdf.configuration.TimePeriod;

public class LimitCollectionsTask extends ChannelTask {
	
    private static Logger logger = LoggerFactory.getLogger(LimitCollectionsTask.class);
	
	private final CollectionAllocator allocator;
	private final DB db;
	private final int maxCount;
	private final long maxTime;
	
	public LimitCollectionsTask(PluginConfiguration config){
		
		this.allocator = config.get(HVDF.ALLOCATOR, CollectionAllocator.class);
		this.db = config.get(HVDF.DB, DB.class);
		this.maxTime = (config.get("by_time", TimePeriod.class, new TimePeriod(0))).getAs(TimeUnit.MILLISECONDS);		
		this.maxCount = (config.get("by_count", Number.class, 0)).intValue();		
	}

	@Override
	public void run() {
		
		try{
			
			// Get a sorted list of slices 
			List<SliceDetails> slices = allocator.getCollectionSlices();
			Collections.sort(slices, SliceDetails.Comparators.MIN_TIME_DESCENDING);
			
			// Trim collections to size if specified
			if(this.maxCount > 0){
				while(slices.size() > this.maxCount){
					int targetIndex = slices.size() - 1;
					this.db.getCollection(slices.get(targetIndex).name).drop();
					slices.remove(targetIndex);
				}
			}
			
			// Now trim by range if applicable
			if(this.maxTime > 0 && slices.size() > 1){
				long baseTime = slices.get(0).minTime;
				for(SliceDetails slice : slices){
					if((baseTime - slice.minTime) > this.maxTime){
						this.db.getCollection(slice.name).drop();					
					}
				}
			}		
		} catch (Exception ex){
			logger.error("Error limiting collections : ", ex);
		}
	}		
}
	
	
