package com.mongodb.hvdf.channel;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.channels.Channel;
import com.mongodb.hvdf.util.HVDFChannelTest;

import org.junit.Test;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class RollupStorageTest extends HVDFChannelTest{
    
    public RollupStorageTest() throws UnknownHostException {
		super();
	}

    @Test
    public void testMaxSingleField() throws Exception {

    	long sampleTime = TimeUnit.MINUTES.toMillis(1);
    	int testSize = 10000;
    	int maxValue = 5;
    	
    	String configPath = "plugin_config/rollup_max_min_count.json";
    	Channel channel = getConfiguredChannel(configPath);
    	
    	for(int i=0; i < testSize; i++){
	    	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, i*sampleTime);
	    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", i % (maxValue + 1)));
	    	sample.append(Sample.SOURCE_KEY, "sensor1");
	    	channel.pushSample(sample, false, new BasicDBList());
    	}
    	
    	
    	// get all the rollup documents
    	List<Sample> samples = channel.query(null, TimeUnit.MINUTES.toMillis(testSize), 
    			TimeUnit.MINUTES.toMillis(testSize), null, null, testSize);
    	
    	// Each document is 60 samples, may be a partial document at end
    	assertEquals((testSize/60) + (testSize%60 > 0 ? 1 : 0), samples.size());
    	
    	// Check every rollup document has a max of maxValue
    	for(Sample rollupDoc : samples){
    		BasicDBObject v = (BasicDBObject)rollupDoc.getData().get("v");
    		assertEquals(maxValue, (int)v.getInt("max"));
    	}    	
    }

    @Test
    public void testMinSingleField() throws Exception {

    	long sampleTime = TimeUnit.MINUTES.toMillis(1);
    	int testSize = 10000;
    	int maxValue = 5;
    	
    	String configPath = "plugin_config/rollup_max_min_count.json";
    	Channel channel = getConfiguredChannel(configPath);
    	
    	for(int i=0; i < testSize; i++){
	    	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, i*sampleTime);
	    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", i % (maxValue + 1)));
	    	sample.append(Sample.SOURCE_KEY, "sensor1");
	    	channel.pushSample(sample, false, new BasicDBList());
    	}    	
    	
    	// get all the rollup documents
    	List<Sample> samples = channel.query(null, TimeUnit.MINUTES.toMillis(testSize), 
    			TimeUnit.MINUTES.toMillis(testSize), null, null, testSize);
    	
    	// Each document is 60 samples, may be a partial document at end
    	assertEquals((testSize/60) + (testSize%60 > 0 ? 1 : 0), samples.size());
    	
    	// Check every rollup document has a min of 0
    	for(Sample rollupDoc : samples){
    		BasicDBObject v = (BasicDBObject)rollupDoc.getData().get("v");
    		assertEquals(0, (int)v.getInt("min"));
    	}    	
    }

    @Test
    public void testRollupCountField() throws Exception {

    	long sampleTime = TimeUnit.MINUTES.toMillis(1);
    	int testSize = 10000;
    	
    	String configPath = "plugin_config/rollup_max_min_count.json";
    	Channel channel = getConfiguredChannel(configPath);
    	
    	for(int i=0; i < testSize; i++){
	    	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, i*sampleTime);
	    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", (testSize - i)));
	    	sample.append(Sample.SOURCE_KEY, "sensor1");
	    	channel.pushSample(sample, false, new BasicDBList());
    	}
    	
    	
    	// get all the rollup documents
    	List<Sample> samples = channel.query(null, TimeUnit.MINUTES.toMillis(testSize), 
    			TimeUnit.MINUTES.toMillis(testSize), null, null, testSize);
    	
    	// Each document is 60 samples, may be a partial document at end
    	assertEquals((testSize/60) + (testSize%60 > 0 ? 1 : 0), samples.size());
    	
    	int totalRollupCount = 0;
    	for(Sample rollupDoc : samples){
    		BasicDBObject v = (BasicDBObject)rollupDoc.getData().get("v");
    		totalRollupCount += (int)v.getInt("count");
    	}
    	
    	assertEquals(testSize, totalRollupCount);    	
    }

    @Test
    public void testRollupGroupCountField() throws Exception {

    	long sampleTime = TimeUnit.MINUTES.toMillis(1);
    	int testSize = 10000;
    	
    	String configPath = "plugin_config/rollup_group_count.json";
    	Channel channel = getConfiguredChannel(configPath);
    	
    	for(int i=0; i < testSize; i++){
	    	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, i*sampleTime);
	    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", i%25));
	    	sample.append(Sample.SOURCE_KEY, "sensor1");
	    	channel.pushSample(sample, false, new BasicDBList());
    	}
    	    	
    	// get all the rollup documents
    	List<Sample> samples = channel.query(null, TimeUnit.MINUTES.toMillis(testSize), 
    			TimeUnit.MINUTES.toMillis(testSize), null, null, testSize);
    	
    	// Each document is 60 samples, may be a partial document at end
    	assertEquals((testSize/60) + (testSize%60 > 0 ? 1 : 0), samples.size());
    	
    	int totalRollupCount = 0;
    	for(Sample rollupDoc : samples){
    		
    		// For each doc, ensure the group count total matches the sample total
    		BasicDBObject v = (BasicDBObject)rollupDoc.getData().get("v");
    		int rollupCount = v.getInt("count");
    		BasicDBObject groups = (BasicDBObject)v.get("group_count");
    		int localTotal = 0;
    		for(String groupKey : groups.keySet()){
    			localTotal += groups.getInt(groupKey);
    		}
    		
        	assertEquals(rollupCount, localTotal);    	    		
    		totalRollupCount += localTotal;
    	}
    	
    	assertEquals(testSize, totalRollupCount);    	
    }
}
