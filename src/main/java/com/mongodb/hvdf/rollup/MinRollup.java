package com.mongodb.hvdf.rollup;

import com.mongodb.hvdf.configuration.PluginConfiguration;

public class MinRollup extends OperatorBasedRollup{
	
	public MinRollup(PluginConfiguration config){
		super(config, "$min", "min");
	}
}
