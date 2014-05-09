package com.mongodb.hvdf.async;

import java.util.TimerTask;

public class RecoveryTimerTask extends TimerTask {

    private final DefaultAsyncService service;

    public RecoveryTimerTask(DefaultAsyncService asyncService) {
        this.service = asyncService;
    }

    @Override
    public void run() {
        service.recoverTasks();
    }

}
