package com.mongodb.hvdf;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.mongodb.MongoClientURI;
import com.mongodb.hvdf.cli.ServerMonitoringSimulatorLoad;
import com.mongodb.hvdf.resources.FeedResource;
import com.yammer.dropwizard.Service;
import com.yammer.dropwizard.config.Bootstrap;
import com.yammer.dropwizard.config.Environment;

public class HVDFService extends Service<HVDFConfiguration> {

	
	@JsonAutoDetect(fieldVisibility=Visibility.NONE, creatorVisibility=Visibility.NONE)
	abstract class IgnoreBasicDBObjMap {}
		  
	
	public static void main(String[] args) throws Exception {
        new HVDFService().run(args);
    }

    @Override
    public void initialize(Bootstrap<HVDFConfiguration> configBootstrap) {
        configBootstrap.setName("hvdf");
        configBootstrap.addCommand( new ServerMonitoringSimulatorLoad() );

    }

    @Override
    public void run(HVDFConfiguration config, Environment environment) throws Exception {
      	        
       	// Get the configured default MongoDB URI
        MongoClientURI default_uri = config.mongodb.default_database_uri;
        
        // Initialize the services as per configuration
        ServiceManager services = new ServiceManager(config.services, default_uri);
        environment.manage(services);
               
        // Register the custom ExceptionMapper to handle ServiceExceptions
        environment.addProvider(new ServiceExceptionMapper());
        
        environment.addResource( new FeedResource( services.getChannelService()) );
    }
}
