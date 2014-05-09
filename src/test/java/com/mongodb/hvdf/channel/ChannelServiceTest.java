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

import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameters;

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class ChannelServiceTest {

    private static final String DATABASE_NAME = 
    		ChannelServiceTest.class.getSimpleName();
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
    
    public ChannelServiceTest(String testName, Map<String, Object> svcConfig) 
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
    public void shouldPushToChannel() throws Exception {

    	String feedName = "feed1";
        MongoClientURI uri = new MongoClientURI(BASE_URI + feedName);
    	DatabaseTools.dropDatabaseByURI(uri, feedName);
    	
    	// put a sample in
    	Channel channel = channelSvc.getChannel(feedName, "channel1");
    	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, 100L);
    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", 1));
    	channel.pushSample(sample, false, new BasicDBList());
    	
    	// get the sample by id
    	List<Sample> samples = channel.query(200, 150, null, null, 50);
    	assertEquals(samples.size(), 1);
    	Sample found = samples.get(0);
    	assertEquals(found.getTimeStamp(), 100);    	
    }

    @Test
    public void shouldQueryAcrossChunks() throws Exception {

    	String feedName = "feed2";
        MongoClientURI uri = new MongoClientURI(BASE_URI + feedName);
    	DatabaseTools.dropDatabaseByURI(uri, feedName);
    	
    	// put a sample in
    	Channel channel = channelSvc.getChannel(feedName, "channel1");
       	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, 100L);
    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", 1));
    	channel.pushSample(sample, false, new BasicDBList());
       	sample = new BasicDBObject(Sample.TS_KEY, 100000);
    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", 2));
    	channel.pushSample(sample, false, new BasicDBList());
    	
    	// get the sample by id
    	List<Sample> samples = channel.query(100050, 99900, null, null, 50);
    	assertEquals(samples.size(), 1);
    	
    	samples = channel.query(99900, 99000, null, null, 50);
    	assertEquals(samples.size(), 0);
    	
    	samples = channel.query(100050, 100000, null, null, 50);
    	assertEquals(samples.size(), 2);
    	
    }

}
