package com.mongodb.hvdf.channels;


public abstract class ChannelTask 
	extends ChannelPlugin 
	implements Runnable{

	public static final String PERIOD_KEY = "period";
	private static final long DEFAULT_TASK_PERIOD = 60000;
	
	private long period = DEFAULT_TASK_PERIOD;

	public long getPeriod() {
		return period;
	}
	
	public void setPeriod(long period) {
		this.period = period;
	}
}
