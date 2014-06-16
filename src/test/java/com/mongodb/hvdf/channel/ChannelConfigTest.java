package com.mongodb.hvdf.channel;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.api.ServiceException;
import com.mongodb.hvdf.channels.Channel;
import com.mongodb.hvdf.util.HVDFChannelTest;
import com.mongodb.hvdf.util.JSONParam;
import com.yammer.dropwizard.testing.JsonHelpers;

import org.junit.Test;
import static org.junit.Assert.*;

import java.net.UnknownHostException;
import java.util.List;

public class ChannelConfigTest extends HVDFChannelTest{
    
    public ChannelConfigTest() throws UnknownHostException {
		super();
	}

	@Test
    public void shouldConfigureChannel() throws Exception {

    	String feedName = "feed1";
    	String channelName = "channel1";
    	String configPath = "plugin_config/valid_config_1.json";
    	
    	getConfiguredChannel(configPath, feedName, channelName);
    	
    	// get the configuration back
    	BasicDBObject config = (BasicDBObject) 
    			channelSvc.getChannelConfiguration(feedName, channelName);
    	config.removeField("_id");
    	JSONParam configReturn = new JSONParam(config);
    	JSONParam originalParam = new JSONParam(JsonHelpers.jsonFixture(configPath));   	

    	assertEquals(originalParam, configReturn);
    }

    @Test(expected=ServiceException.class)
    public void shouldFailBadPluginClass() throws Exception {

    	String feedName = "feed2";
    	String channelName = "channel1";
    	String configPath = "plugin_config/invalid_config_interceptors_bad_class.json";   	
    	
    	// Try to configure
    	getConfiguredChannel(configPath, feedName, channelName);
    }

    @Test(expected=ServiceException.class)
    public void shouldFailNoPluginClass() throws Exception {

    	String feedName = "feed2";
    	String channelName = "channel1";
    	String configPath = "plugin_config/invalid_config_interceptors_no_class.json";   	
    	    	
    	// Try to configure
    	getConfiguredChannel(configPath, feedName, channelName);
    }

    @Test
    public void testInterceptorOrdering() throws Exception {

    	String feedName = "feed3";
    	String channelName = "channel1";
    	String configPath = "plugin_config/interceptor_ordering.json";   	
    	
    	// Try to configure
    	Channel channel = getConfiguredChannel(configPath, feedName, channelName);

    	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, 100L);
    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", 240));
    	channel.pushSample(sample, false, new BasicDBList());
    	
    	List<Sample> recalled = channel.query(null, 1000, 1000, null, null, 1);
    	
    	// The interceptors should have added the field x and then posted the values [2,1]
    	BasicDBList testList = (BasicDBList) recalled.get(0).getData().get("x");
    	assertEquals(testList.get(0), 2);
    	assertEquals(testList.get(1), 1);   	
    }
}
