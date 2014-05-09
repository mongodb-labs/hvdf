package com.mongodb.hvdf;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.mongodb.MongoClientURI;
import com.mongodb.hvdf.services.AsyncService;
import com.mongodb.hvdf.services.ChannelService;
import com.mongodb.hvdf.services.Service;
import com.yammer.dropwizard.lifecycle.Managed;

public class ServiceManager implements Managed{

    private static Logger logger = LoggerFactory.getLogger(ServiceManager.class);

    public static final String MODEL_KEY = "model";
    public static final String ASYNC_SERVICE_KEY = "async_service";
    public static final String CHANNEL_SERVICE_KEY = "channel_service";
    private static final String DEFAULT_ASYNC_SERVICE = null;

    private static final long SERVICE_SHUTDOWN_TIMEOUT = 30; // Seconds;

	private static final String DEFAULT_CHANNEL_SERVICE = "DefaultChannelService";
    
    private final Map<String, Object> svcConfig;
    private final ServiceFactory factory;
    private final MongoClientURI defaultDbUri;

    public ServiceManager(Map<String, Object> svcConfig, MongoClientURI defaultUri) {

        this.svcConfig = svcConfig;
        this.factory = new ServiceFactory();
        this.defaultDbUri = defaultUri;
        
        logger.info("Initializing configured services");
        // Load the configured AsyncService implementation
        Map<String, Object> asyncServiceConfig = getServiceConfig(ASYNC_SERVICE_KEY, DEFAULT_ASYNC_SERVICE);
        if(asyncServiceConfig != null){
            factory.createAndRegisterService(
                    AsyncService.class, asyncServiceConfig, this.defaultDbUri);
        }
        
        // Load the configured UserGraphService implementation
        Map<String, Object> channelServiceConfig = getServiceConfig(CHANNEL_SERVICE_KEY, DEFAULT_CHANNEL_SERVICE);
        factory.createAndRegisterService(
                ChannelService.class, channelServiceConfig, this.defaultDbUri);
                
    }
    
    public AsyncService getAsyncService() {
        return factory.getService(AsyncService.class);
    }

    public ChannelService getChannelService() {
        return factory.getService(ChannelService.class);
    }

    
    @SuppressWarnings("unchecked")
    private Map<String, Object> getServiceConfig(final String serviceKey, final String defaultServiceKey){
        Map<String, Object> configItem = (Map<String, Object>) svcConfig.get(serviceKey);
        
        if(configItem == null && defaultServiceKey != null){
            configItem = new LinkedHashMap<String, Object>();
            configItem.put(MODEL_KEY, defaultServiceKey);
        }
        
        return configItem;
    }

    @Override
    public void start() throws Exception {
    }

    @Override
    public void stop() throws Exception {
        logger.info("Stopping configured services");            
        List<? extends Service> services = factory.getServiceList();

        // If there is an async service, stop it first to avoid
        // async tasks firing during other service shutdown
        Service asyncService = null;
        try{asyncService = this.getAsyncService();} catch(Exception e){}
        if(asyncService != null){
            asyncService.shutdown(SERVICE_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
            services.remove(asyncService);
        }
        
        for(Service service : services)
            service.shutdown(SERVICE_SHUTDOWN_TIMEOUT, TimeUnit.SECONDS);
        logger.info("All services shut down successfully");
    }

}
