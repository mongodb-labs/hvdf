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

public class ChannelServiceTest extends HVDFChannelTest{

    public ChannelServiceTest() throws UnknownHostException {
		super();
	}

	@Test
    public void shouldPushToChannel() throws Exception {

    	String feedName = "feed1";
    	String channelName = "channel1";
    	
    	Channel channel = getConfiguredChannel(null, feedName, channelName);
    	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, 100L);
    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", 1));
    	channel.pushSample(sample, false, new BasicDBList());
    	
    	// get the sample by id
    	List<Sample> samples = channel.query(null, 200, 150, null, null, 50);
    	assertEquals(samples.size(), 1);
    	Sample found = samples.get(0);
    	assertEquals(found.getTimeStamp(), 100);    	
    }

    @Test
    public void shouldQueryAcrossChunks() throws Exception {

    	String feedName = "feed2";
    	String channelName = "channel1";
    	
    	Channel channel = getConfiguredChannel(null, feedName, channelName);
       	BasicDBObject sample = new BasicDBObject(Sample.TS_KEY, 100L);
    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", 1));
    	channel.pushSample(sample, false, new BasicDBList());
       	sample = new BasicDBObject(Sample.TS_KEY, 100000);
    	sample.append(Sample.DATA_KEY, new BasicDBObject("v", 2));
    	channel.pushSample(sample, false, new BasicDBList());
    	
    	// get the sample by id
    	List<Sample> samples = channel.query(null, 100050, 99900, null, null, 50);
    	assertEquals(samples.size(), 1);
    	
    	samples = channel.query(null, 99900, 99000, null, null, 50);
    	assertEquals(samples.size(), 0);
    	
    	samples = channel.query(null, 100050, 100000, null, null, 50);
    	assertEquals(samples.size(), 2);
    	
    }

}
