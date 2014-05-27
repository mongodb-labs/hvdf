package com.mongodb.hvdf.util;

import java.io.IOException;
import java.net.UnknownHostException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.junit.After;
import org.junit.Rule;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.hvdf.ServiceFactory;
import com.mongodb.hvdf.ServiceManager;
import com.mongodb.hvdf.channels.Channel;
import com.mongodb.hvdf.services.ChannelService;
import com.yammer.dropwizard.testing.JsonHelpers;

public class HVDFChannelTest {

	@Rule public TestName name = new TestName();
	private static final String BASE_URI = "mongodb://localhost/";
    private static final String CONFIG_DB_NAME = "config";
    private static final String CONFIG_COLL_PREFIX = "hvdf_channels_";

    protected final MongoClient testClient;

    protected final Logger logger = LoggerFactory.getLogger(this.getClass());   
    protected final String databaseName = this.getClass().getSimpleName();

    protected final ChannelService channelSvc;

    
    @After
    public void tearDown() throws Exception {
    	this.channelSvc.shutdown(10, TimeUnit.SECONDS);
    	this.testClient.close();
    }

    public HVDFChannelTest() 
            throws UnknownHostException {
        
        Map<String, Object> defaultConfig = new LinkedHashMap<String, Object>();
        defaultConfig.put(ServiceManager.MODEL_KEY, "DefaultChannelService");
        

        MongoClientURI uri = new MongoClientURI(BASE_URI + databaseName);
        testClient = new MongoClient(uri);
        testClient.dropDatabase(databaseName);
        
        // Load the configured ContentService implementation 
        ServiceFactory factory = new ServiceFactory();
        this.channelSvc = factory.createService(ChannelService.class, defaultConfig, uri);
    }
    
    public Channel getConfiguredChannel(String configPath, 
    		String feedName, String channelName) throws IOException{
    	
    	// manually remove any previous config for the feed
    	testClient.dropDatabase(feedName);
    	testClient.getDB(CONFIG_DB_NAME).getCollection(CONFIG_COLL_PREFIX + feedName).drop();
    	
    	// If the configuration is passed, use it otherwise use empty
    	JSONParam configParam = null;
    	if(configPath != null){
        	configParam = new JSONParam(JsonHelpers.jsonFixture(configPath));
    	} else {
    		configParam = new JSONParam("{}");
    	}
    	
    	this.channelSvc.configureChannel(feedName, channelName, configParam);
		return channelSvc.getChannel(feedName, channelName);
    	
    }
    
	public Channel getConfiguredChannel(String configPath, String channelName) 
			throws IOException{
    	
		// no feed name, so just use the test class name as the feed
		return getConfiguredChannel(configPath, this.databaseName, channelName);
    }
    
    public Channel getConfiguredChannel(String configPath) 
    		throws IOException{
    	
    	// no channel name, derive from test method
    	return getConfiguredChannel(configPath, name.getMethodName());
    }
        
}
