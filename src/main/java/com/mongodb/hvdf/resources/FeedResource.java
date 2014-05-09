package com.mongodb.hvdf.resources;
import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.channels.Channel;
import com.mongodb.hvdf.services.ChannelService;
import com.mongodb.hvdf.util.JSONParam;
import com.mongodb.util.JSON;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

import java.util.List;

@Path("/feed")
@Produces(MediaType.APPLICATION_JSON)
public class FeedResource {

    private final ChannelService channelService;

    public FeedResource(ChannelService feed) {
        this.channelService = feed;
    }


    @DELETE
    @Path("/{feed}/{channel}/data/{id}")
    public void deleteFromChannel(@PathParam("user_id") String user_id,
            @PathParam("feed") String feedId,
            @PathParam("channel") String channelId,
            @QueryParam("id") String sampleId) {
    	
    	// Find the right channel
    	Channel channel = channelService.getChannel(feedId, channelId);
 
    	// Remove from the channel
    	channel.removeSample(sampleId);
    }

    @POST
    @Path("/{feed}/{channel}/data")
    public String pushToChannel(
            @PathParam("feed") String feedId,
            @PathParam("channel") String channelId,
            @QueryParam("sample") JSONParam sample ) {

        // Find the correct channel implementation
    	Channel channel = channelService.getChannel(feedId, channelId);
    	    	
        // push it to the channel correct
    	DBObject sampleObj = sample.toDBObject();
    	
    	BasicDBList sid = new BasicDBList();
    	channel.pushSample(sampleObj, sampleObj instanceof BasicDBList, sid);
        
        // return the ID
        return JSON.serialize(sid);
    }

    @GET
    @Path("/{feed}/{channel}/data")
    public List<Sample> queryChannel(
            @PathParam("feed") String feedId,
            @PathParam("channel") String channelId,
            @QueryParam("ts") long timeStart,
            @QueryParam("range") long timeRange,
            @QueryParam("query") JSONParam query,
        	@QueryParam("proj") JSONParam projection,
        	@QueryParam("limit") int limit) {

        // Find the correct channel implementation
    	Channel channel = channelService.getChannel(feedId, channelId);
    	    	
        // push it to the channel correct
    	DBObject dbQuery = query != null ? query.toDBObject() : null;
    	DBObject dbProjection = projection != null ? projection.toDBObject() : null;
    	return channel.query(timeStart, timeRange, dbQuery, dbProjection, limit);    
    }

    @PUT
    @Path("/{feed}/{channel}/config")
    public void configureChannel(
            @PathParam("feed") String feedId,
            @PathParam("channel") String channelId,
            @QueryParam("value") JSONParam config) {

        // Find the correct channel implementation
    	channelService.configureChannel(feedId, channelId, config);
    }
}
