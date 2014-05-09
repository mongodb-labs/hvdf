package com.mongodb.hvdf.cli;

import com.mongodb.BasicDBObject;
import com.mongodb.MongoClientURI;
import com.mongodb.hvdf.HVDFConfiguration;
import com.mongodb.hvdf.ServiceManager;
import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.api.Source;
import com.mongodb.hvdf.resources.FeedResource;
import com.mongodb.hvdf.util.JSONParam;
import com.yammer.dropwizard.cli.ConfiguredCommand;
import com.yammer.dropwizard.config.Bootstrap;

import net.sourceforge.argparse4j.inf.Namespace;
import net.sourceforge.argparse4j.inf.Subparser;

import java.util.Date;
import java.util.Random;
import java.util.concurrent.TimeUnit;

public class ServerMonitoringSimulatorLoad extends ConfiguredCommand<HVDFConfiguration> {

    public ServerMonitoringSimulatorLoad() {
        super("server-monitoring-simulator", "Simulates agents reporting server load and clients querying");
    }

    @Override
    public void configure(Subparser subparser) {
        super.configure(subparser);
        subparser.addArgument("--servers").required(true).type(Integer.class);
        subparser.addArgument("--days").required(true).type(Integer.class);
        subparser.addArgument("--period").required(false).type(Integer.class);
    }

    @Override
    public void run(Bootstrap<HVDFConfiguration> configurationBootstrap,  Namespace namespace,
                    HVDFConfiguration configuration ) {

        int numServers = namespace.getInt("servers");
        int numDays  = namespace.getInt("days");
        
        // Sample period 5mins by default
        long period = TimeUnit.SECONDS.toMillis(5*60);
        Integer periodCfg = namespace.getInt("period");
        if(periodCfg != null)
            period = TimeUnit.SECONDS.toMillis(periodCfg);
        
        System.out.println("Simulating " + numServers + " over " + numDays + " days");

        MongoClientURI default_uri = configuration.mongodb.default_database_uri;
        ServiceManager services = new ServiceManager(configuration.services, default_uri);
        FeedResource feedResource = new FeedResource( services.getChannelService() );
        Random rand = new Random();

        long endOfTime = (new Date()).getTime();
        long beginningOfTime = endOfTime - TimeUnit.DAYS.toMillis(numDays);

        for( long clock = beginningOfTime; clock < endOfTime; clock += period) {
            for( int serverId = 0; serverId < numServers; serverId++ ) {
            	BasicDBObject sample = new BasicDBObject();
            	sample.append(Sample.TS_KEY, clock).append(Sample.SOURCE_KEY, "server" + serverId);
            	sample.append(Sample.DATA_KEY, new BasicDBObject("load", rand.nextInt(100)));
                feedResource.pushToChannel("servers", "load", new JSONParam(sample));
            }
        }
    }
}