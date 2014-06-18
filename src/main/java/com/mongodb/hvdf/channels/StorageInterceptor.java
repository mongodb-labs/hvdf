package com.mongodb.hvdf.channels;

import com.mongodb.BasicDBObject;
import com.mongodb.hvdf.configuration.PluginConfiguration;
import com.mongodb.hvdf.oid.HiDefTimeIdFactory;
import com.mongodb.hvdf.oid.SampleIdFactory;

public abstract class StorageInterceptor extends ChannelInterceptor {

	private static final String ID_FACTORY_KEY = "id_factory";
	private static final PluginConfiguration DEFAULT_ID_FACTORY = 
			new PluginConfiguration(new BasicDBObject(PluginFactory.TYPE_KEY, 
					HiDefTimeIdFactory.class.getName()), StorageInterceptor.class);

	protected final SampleIdFactory idFactory;
	
	protected StorageInterceptor(PluginConfiguration config) {
		
		// check if the id_type has been specifically configured
		PluginConfiguration idFactoryConfig = config.get(
				ID_FACTORY_KEY, PluginConfiguration.class, getDefaultIdFactoryConfig());
		this.idFactory = PluginFactory.loadPlugin(SampleIdFactory.class, idFactoryConfig);
		
	}

	protected PluginConfiguration getDefaultIdFactoryConfig(){
		return DEFAULT_ID_FACTORY;
	}
	
	public SampleIdFactory getIdFactory() {
		return this.idFactory;
	}

}
