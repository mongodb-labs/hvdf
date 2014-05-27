package com.mongodb.hvdf.configuration;

public class ChannelServiceConfiguration extends MongoServiceConfiguration {
	
    /**
     * If a channel has not been configured in the system, should
     * a default channel be created 
     */
    public boolean allow_unconfigured_channels = true;
	
    /**
     * Default length of time a collection is used for incoming data before
     * it is pushed to historical collection list
     */
    public long collection_time_slice = 20*1000;
	
    /**
     * If channel configuration exists prior to server starting, create
     * and prepare all configured channel before making the feed service available
     */
    public boolean preload_configured_channels = true;

    /**
     * The channel service manages the scheduling and execution of periodic
     * channel tasks. Sets the maximum number of threads assigned to executing these.
     */
	public int channel_task_thread_pool_size = 2;
}
