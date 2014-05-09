package com.mongodb.hvdf;

import java.util.LinkedHashMap;
import java.util.Map;

import com.mongodb.hvdf.configuration.MongoGeneralConfiguration;
import com.yammer.dropwizard.config.Configuration;

public class HVDFConfiguration extends Configuration {
    
    public MongoGeneralConfiguration mongodb = new MongoGeneralConfiguration();
    public Map<String, Object> services = new LinkedHashMap<String, Object>();
    
}
