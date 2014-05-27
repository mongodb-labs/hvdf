package com.mongodb.hvdf.channels;

import java.util.concurrent.ScheduledFuture;

public interface ChannelTaskScheduler {
	
	public ScheduledFuture<?> scheduleTask(
			String feedName, String channelName, 
			ChannelTask task, long msPeriod);
	
	public void cancelAllForChannel(String feedName, String channelName);
	
}
