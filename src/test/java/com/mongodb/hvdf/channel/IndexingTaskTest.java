package com.mongodb.hvdf.channel;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.channels.Channel;
import com.mongodb.hvdf.util.HVDFChannelTest;

import org.junit.Test;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.TimeUnit;

public class IndexingTaskTest extends HVDFChannelTest{


    public IndexingTaskTest() throws UnknownHostException {
		super();
	}

    @Test
    public void shouldIndexAllCollections() throws Exception {

    	String feedName = "feed1";
    	String channelName = "channel1";
    	String configPath = "plugin_config/indexing_task_all_channels.json";

    	
    	Channel channel = getConfiguredChannel(configPath, feedName, channelName);
    	pushDataToChannel(channel, "v", 5000, 1, TimeUnit.SECONDS);

    	// Wait for the task to complete
    	Thread.sleep(2000);
    	
    	// Get the collections for the feed
    	DB feedDB = this.testClient.getDB(feedName);
    	Set<String> collNames = feedDB.getCollectionNames();
		assertEquals("Should have 5 data collections + system.indexes", 6, collNames.size());
    	for(String collName : collNames){
    		if(collName.equals("system.indexes") == false){
    			DBCollection coll = feedDB.getCollection(collName);
    			List<DBObject> indexes = coll.getIndexInfo();
    			assertEquals("Should have _id index plus one additional", 2, indexes.size());
    			assertEquals("Should have data.v_1 index", indexes.get(1).get("name"), "data.v_1");
    		}
    	}
    }


    @Test
    public void shouldIndexSomeCollections() throws Exception {

    	String feedName = "feed2";
    	String channelName = "channel1";
    	String configPath = "plugin_config/indexing_task_skip_channels.json";

    	
    	Channel channel = getConfiguredChannel(configPath, feedName, channelName);
    	pushDataToChannel(channel, "v", 5000, 1, TimeUnit.SECONDS);

    	// Wait for the task to complete
    	Thread.sleep(2000);
    	
    	// Get the collections for the feed
    	DB feedDB = this.testClient.getDB(feedName);
    	Set<String> collNames = feedDB.getCollectionNames();
		assertEquals("Should have 5 data collections + system.indexes", 6, collNames.size());
		
		int indexedCount = 0;
    	for(String collName : collNames){
    		if(collName.equals("system.indexes") == false){
    			DBCollection coll = feedDB.getCollection(collName);
    			List<DBObject> indexes = coll.getIndexInfo();
    			if(indexes.size() == 2){
    				assertEquals("Should have data.v_1 index", indexes.get(1).get("name"), "data.v_1");
    				indexedCount++;
    			}
    		}
    	}
    	
		assertEquals("Should 3 indexed collections", 3, indexedCount);    	
    }


    private void pushDataToChannel(Channel channel, 
    		String field, int numSamples, int samplePeriod, TimeUnit unit) {
    	
    	
    	long periodMs = TimeUnit.MILLISECONDS.convert(samplePeriod, unit);
    	
    	for(int i=0; i < numSamples; i++){
	    	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, i*periodMs);
	    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", i));
	    	channel.pushSample(sample, false, new BasicDBList());
    	}
    	
	}
}
