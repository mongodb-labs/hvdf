package com.mongodb.hvdf.rollup;

import com.mongodb.hvdf.configuration.PluginConfiguration;

public class TotalRollup extends OperatorBasedRollup{
	
	public TotalRollup(PluginConfiguration config){
		super(config, "$inc", "total");
	}
}
