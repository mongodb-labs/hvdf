package com.mongodb.hvdf.rollup;

import com.mongodb.hvdf.configuration.PluginConfiguration;

public class AddToSetRollup extends OperatorBasedRollup{
	
	public AddToSetRollup(PluginConfiguration config){
		super(config, "$addToSet", "set");
	}
}
