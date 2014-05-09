package com.mongodb.hvdf.channel;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hvdf.ServiceFactory;
import com.mongodb.hvdf.ServiceManager;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.api.ServiceException;
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

import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

@RunWith(Parameterized.class)
public class ChannelConfigTest {

    private static final String DATABASE_NAME = 
    		ChannelConfigTest.class.getSimpleName();
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
    
    public ChannelConfigTest(String testName, Map<String, Object> svcConfig) 
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
    public void shouldConfigureChannel() throws Exception {

    	String feedName = "feed1";
    	String channelName = "channel1";
        MongoClientURI uri = new MongoClientURI(BASE_URI + feedName);
    	DatabaseTools.dropDatabaseByURI(uri, feedName);
    	
    	// Load a valid config from test resource
    	JSONParam configParam = new JSONParam(
    			JsonHelpers.jsonFixture("plugin_config/valid_config_1.json"));   	
    	
    	// call the channel configuration
    	channelSvc.configureChannel(feedName, channelName, configParam);
    	
    	// get the configuration back
    	BasicDBObject config = (BasicDBObject) 
    			channelSvc.getChannelConfiguration(feedName, channelName);
    	config.removeField("_id");
    	JSONParam configReturn = new JSONParam(config);

    	assertEquals(configParam, configReturn);
    }

    @Test(expected=ServiceException.class)
    public void shouldFailBadPluginClass() throws Exception {

    	String feedName = "feed2";
    	String channelName = "channel1";
        MongoClientURI uri = new MongoClientURI(BASE_URI + feedName);
    	DatabaseTools.dropDatabaseByURI(uri, feedName);
    	
    	// Load an invalid config from test resource
    	JSONParam configParam = new JSONParam(
    			JsonHelpers.jsonFixture("plugin_config/invalid_config_interceptors_bad_class.json"));   	
    	
    	// Try to configure
    	channelSvc.configureChannel(feedName, channelName, configParam);
    	
    	channelSvc.getChannel(feedName, channelName);
    }

    @Test(expected=ServiceException.class)
    public void shouldFailNoPluginClass() throws Exception {

    	String feedName = "feed2";
    	String channelName = "channel1";
        MongoClientURI uri = new MongoClientURI(BASE_URI + feedName);
    	DatabaseTools.dropDatabaseByURI(uri, feedName);
    	
    	// Load an invalid config from test resource
    	JSONParam configParam = new JSONParam(
    			JsonHelpers.jsonFixture("plugin_config/invalid_config_interceptors_no_class.json"));   	
    	
    	// Try to configure
    	channelSvc.configureChannel(feedName, channelName, configParam);
    	
    	// Try to create
    	channelSvc.getChannel(feedName, channelName);
    }

    @Test
    public void testInterceptorOrdering() throws Exception {

    	String feedName = "feed3";
    	String channelName = "channel1";
        MongoClientURI uri = new MongoClientURI(BASE_URI + feedName);
    	DatabaseTools.dropDatabaseByURI(uri, feedName);
    	
    	// Load an invalid config from test resource
    	JSONParam configParam = new JSONParam(
    			JsonHelpers.jsonFixture("plugin_config/interceptor_ordering.json"));   	
    	
    	// Try to configure
    	channelSvc.configureChannel(feedName, channelName, configParam);
    	
    	// Try to create
    	Channel channel = channelSvc.getChannel(feedName, channelName);
       	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, 100L);
    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", 240));
    	channel.pushSample(sample, false, new BasicDBList());
    	
    	List<Sample> recalled = channel.query(1000, 1000, null, null, 1);
    	
    	// The interceptors should have added the field x and then posted the values [2,1]
    	BasicDBList testList = (BasicDBList) recalled.get(0).getData().get("x");
    	assertEquals(testList.get(0), 2);
    	assertEquals(testList.get(1), 1);
    	
    	
    }
}
