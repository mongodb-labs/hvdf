package com.mongodb.hvdf.channel;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hvdf.ServiceFactory;
import com.mongodb.hvdf.ServiceManager;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.channels.Channel;
import com.mongodb.hvdf.services.ChannelService;
import com.mongodb.hvdf.util.DatabaseTools;
import com.mongodb.hvdf.util.JSONParam;
import com.yammer.dropwizard.testing.JsonHelpers;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class BatchSampleTest {

    private static Logger logger = LoggerFactory.getLogger(BatchSampleTest.class);

    private static final String DATABASE_NAME = 
    		BatchSampleTest.class.getSimpleName();
    private static final String BASE_URI = "mongodb://localhost/";

    private ChannelService channelSvc;

    @Parameters
    public static Collection<Object[]> createInputValues() {
        
        Map<String, Object> defaultChannel = new LinkedHashMap<String, Object>();
        defaultChannel.put(ServiceManager.MODEL_KEY, "DefaultChannelService");
                
        // Build the set of test params for the above configs           
        return Arrays.asList(new Object[][] {
            /*[0]*/ {"defaultContent", defaultChannel}               
        });
    }
    
    public BatchSampleTest(String testName, Map<String, Object> svcConfig) 
            throws UnknownHostException {
        
        String databaseName = DATABASE_NAME + "-" + testName;
        MongoClientURI uri = new MongoClientURI(BASE_URI + databaseName);
        DatabaseTools.dropDatabaseByURI(uri, databaseName);
        
        // Load the configured ContentService implementation 
        ServiceFactory factory = new ServiceFactory();
        this.channelSvc = factory.createService(ChannelService.class, svcConfig, uri);
    }
    
    @Before
    public void setUp() throws Exception {
    }

    @After
    public void tearDown() throws Exception {
    	this.channelSvc.shutdown(10, TimeUnit.SECONDS);
    }

    @Test
    public void shouldPushBatchToChannel() throws Exception {

    	String feedName = "feed1";
    	long sampleTime = TimeUnit.HOURS.toMillis(1);
    	int batchSize = 1000;
        MongoClientURI uri = new MongoClientURI(BASE_URI + feedName);
    	DatabaseTools.dropDatabaseByURI(uri, feedName);
    	
    	// put a sample in
    	Channel channel = channelSvc.getChannel(feedName, "channel1");
    	
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

    	String feedName = "feed2";
    	long sampleTime = TimeUnit.HOURS.toMillis(1);
    	int testSize = 10000;
        MongoClientURI uri = new MongoClientURI(BASE_URI + feedName);
    	DatabaseTools.dropDatabaseByURI(uri, feedName);
    	
    	JSONParam configParam = new JSONParam(
    			JsonHelpers.jsonFixture("plugin_config/batching_channel_config.json"));
    	channelSvc.configureChannel(feedName, "channel1", configParam);
    	Channel channel = channelSvc.getChannel(feedName, "channel1");
    	
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

    @Test
    public void shouldPushJSONBatchToChannel() throws Exception {
    	
    }


}
