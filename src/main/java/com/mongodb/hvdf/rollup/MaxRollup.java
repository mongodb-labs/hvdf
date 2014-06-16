package com.mongodb.hvdf.rollup;

import com.mongodb.hvdf.configuration.PluginConfiguration;

public class MaxRollup extends OperatorBasedRollup{
	
	public MaxRollup(PluginConfiguration config){
		super(config, "$max", "max");
	}
}
