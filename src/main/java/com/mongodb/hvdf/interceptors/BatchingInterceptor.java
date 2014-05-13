package com.mongodb.hvdf.interceptors;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.BasicDBList;
import com.mongodb.DBObject;
import com.mongodb.hvdf.channels.ChannelInterceptor;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.configuration.TimePeriod;

public class BatchingInterceptor extends ChannelInterceptor{

    private static Logger logger = LoggerFactory.getLogger(BatchingInterceptor.class);
	
	// Configuration defaults
	private static final int DEFAULT_BATCH_SIZE = 1000;
	private static final int DEFAULT_NTHREADS = 2;
	private static final int DEFAULT_MAX_BATCHES = 1000;
	private static final TimePeriod DEFAULT_MAX_AGE = new TimePeriod(1000);
	
	// Configuration values
	private int maxBatchSize = DEFAULT_BATCH_SIZE;
	private long maxBatchAge = DEFAULT_MAX_AGE.getAs(TimeUnit.MILLISECONDS);
	private int maxQueuedBatches = DEFAULT_MAX_BATCHES;
	private int nThreads = DEFAULT_NTHREADS;

	// Batch management
	private final ScheduledExecutorService batchTimer;
	private final BatchManagementTask stagingBatch;
	private BlockingQueue<Runnable> batchQueue;
	private ThreadPoolExecutor executor;
	
	public BatchingInterceptor(PluginConfiguration config){
		this.batchTimer = Executors.newScheduledThreadPool(1);
		this.stagingBatch = new BatchManagementTask();
		configure(config);
	}
		
	@Override
	public void pushSample(DBObject sample, boolean isList, BasicDBList resultIds) {

		// push the incoming sample(s) to the staging batch
		this.stagingBatch.pushToBatch(sample, isList);
			
		// Do NOT call forward interceptor, this is done
		// within the batch executor
	}

	@Override
	public void shutdown() {
		
		// Orderly shutdown, existing batches will flush
		this.batchTimer.shutdown();
		this.executor.shutdown();
	}	
	
	private void configure(PluginConfiguration config) {
		
		// Pull any specific config from the overrides
		this.maxBatchSize = config.get("target_batch_size", Integer.class, DEFAULT_BATCH_SIZE);
		this.maxBatchAge = (config.get("max_batch_age", TimePeriod.class, DEFAULT_MAX_AGE)).getAs(TimeUnit.MILLISECONDS);
		this.nThreads = config.get("thread_count", Integer.class, DEFAULT_NTHREADS);
		this.maxQueuedBatches = config.get("max_queued_batches", Integer.class, DEFAULT_MAX_BATCHES);
				
		// Schedule the batching task
		this.batchTimer.scheduleAtFixedRate(stagingBatch, 
				maxBatchAge, maxBatchAge, TimeUnit.MILLISECONDS);
		
		// Create the batch queue and the executor that processes them
		this.batchQueue = new LinkedBlockingDeque<Runnable>(this.maxQueuedBatches);
		this.executor = new ThreadPoolExecutor(
				this.nThreads, this.nThreads, 1,
			    TimeUnit.SECONDS, this.batchQueue,
			    new ThreadPoolExecutor.CallerRunsPolicy());
	}
	
	// Task class for sending batches to channel
	private class BatchProcessingTask implements Runnable{
		
		private final BasicDBList batch;
		
		public BatchProcessingTask(final BasicDBList batch){
			this.batch = batch;
		}

		@Override
		public void run() {
			next.pushSample(batch, true, new BasicDBList());
		}
	}
	
	// Task class for managing incoming samples and periodically
	// timing out partial batches
	private class BatchManagementTask implements Runnable
	{
		private BasicDBList currentBatch = new BasicDBList();

		@Override
		public void run() {
			
			// Push a null sample to the task to force a flush
			pushToBatch(null, false);			
		}		
		
		public synchronized void pushToBatch(DBObject sample, boolean isList){
			
			if(sample == null){
				// A null sample indicates a flush is required
				if(currentBatch.size() > 0){
					flushBatch();
				}
			} else {
				
				// Add the list or document to the current batch
				if(isList){
					currentBatch.addAll((BasicDBList) sample);
				} else {
					currentBatch.add(sample);
				}
				
				// If the batch size exceeds limit, flush it
				if(currentBatch.size() >= maxBatchSize){
					flushBatch();
				}
			}
		}

		private void flushBatch() {
			
			// Create an executor task to process 
			logger.debug("Queuing batch for processing - size : {}, queuelength : {}",
					currentBatch.size(), batchQueue.size());
			executor.submit(new BatchProcessingTask(currentBatch));
			currentBatch = new BasicDBList();
		}		
	}
}
