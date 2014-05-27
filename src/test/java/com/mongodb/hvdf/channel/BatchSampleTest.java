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

public class BatchSampleTest extends HVDFChannelTest{
    
    public BatchSampleTest() throws UnknownHostException {
		super();
	}

	@Test
    public void shouldPushBatchToChannel() throws Exception {

    	long sampleTime = TimeUnit.HOURS.toMillis(1);
    	int batchSize = 1000;
    	
    	// put a sample in using default config
    	Channel channel = getConfiguredChannel(null);
    	
    	// Construct a large batch
    	BasicDBList batch = new BasicDBList();
    	for(int i=0; i < batchSize; i++){
	    	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, (i+1)*sampleTime);
	    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", i));
	    	batch.add(sample);
    	}  	
    	
    	channel.pushSample(batch, true, new BasicDBList());
    	
    	// get the sample by id
    	List<Sample> samples = channel.query(TimeUnit.DAYS.toMillis(50), TimeUnit.DAYS.toMillis(50), null, null, 1000);
    	assertEquals(samples.size(), 1000);
    	Sample found = samples.get(0);
    	assertEquals(found.getTimeStamp(), TimeUnit.HOURS.toMillis(batchSize));    	
    }

    @Test
    public void shouldPushToBatchingChannel() throws Exception {

    	long sampleTime = TimeUnit.HOURS.toMillis(1);
    	int testSize = 10000;
    	
    	String configPath = "plugin_config/batching_channel_config.json";
    	Channel channel = getConfiguredChannel(configPath);
    	
    	for(int i=0; i < testSize; i++){
	    	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, (i+1)*sampleTime);
	    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", i));
	    	channel.pushSample(sample, false, new BasicDBList());
    	}
    	
    	Thread.sleep(testSize/2);
    	
    	// get the sample by id
    	List<Sample> samples = channel.query(TimeUnit.DAYS.toMillis(testSize/20), 
    			TimeUnit.DAYS.toMillis(testSize/20), null, null, testSize);
    	assertEquals(samples.size(), testSize);
    	Sample found = samples.get(0);
    	assertEquals(found.getTimeStamp(), TimeUnit.HOURS.toMillis(testSize));    	
    }
}
