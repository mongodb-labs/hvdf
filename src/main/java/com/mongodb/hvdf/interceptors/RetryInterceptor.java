package com.mongodb.hvdf.interceptors;

import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.hvdf.channels.ChannelInterceptor;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.configuration.TimePeriod;

public class RetryInterceptor extends ChannelInterceptor{

    private static Logger logger = LoggerFactory.getLogger(RetryInterceptor.class);

    private static final Integer DEFAULT_MAX_RETRIES = 5;
	private static final TimePeriod DEFAULT_RETRY_TIME = new TimePeriod(3000);
	
	private final Integer maxRetries;
	private final long retryPeriod;

	public RetryInterceptor(PluginConfiguration config){
		
		// Pull any specific config from the overrides
		this.maxRetries = config.get("max_retries", Integer.class, DEFAULT_MAX_RETRIES);
		this.retryPeriod = (config.get("retry_period", TimePeriod.class, DEFAULT_RETRY_TIME)).getAs(TimeUnit.MILLISECONDS);
	}
		
	@Override
	public void pushSample(DBObject sample, boolean isList, BasicDBList resultIds) {
		
		int retryCount = 0;
		while(true){
			try{
				this.next.pushSample(sample, isList, resultIds);
				return;
			} catch(Throwable ex){
				if(retryCount < this.maxRetries){
					
					// count the retry attempt and log failure
					retryCount++;
					logger.warn("Pushing sample failed, retrying in {}ms : {}", this.retryPeriod, ex.toString());
					try { Thread.sleep(this.retryPeriod); } 
					catch (InterruptedException e) {}
				}
				else {
					
					// retry limit reached, throw to previous interceptor
					logger.error("Pushing sample failed after {} attempts : {}", this.maxRetries, ex);
					throw ex;
				}
			} 		
		}	
		
	}	
}
