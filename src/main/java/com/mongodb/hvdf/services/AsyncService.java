package com.mongodb.hvdf.services;

import com.mongodb.hvdf.async.AsyncTaskType;
import com.mongodb.hvdf.async.AsyncWorker;
import com.mongodb.hvdf.async.RecoverableAsyncTask;

public interface AsyncService extends Service {

    // Task management
    public void submitTask(RecoverableAsyncTask task);
    public void taskComplete(RecoverableAsyncTask task);
    public void taskFailed(RecoverableAsyncTask task, Throwable cause);
    public void taskRejected(RecoverableAsyncTask task);

    // Service management
    public void registerRecoveryService(AsyncTaskType taskType, AsyncWorker worker);
     
}
