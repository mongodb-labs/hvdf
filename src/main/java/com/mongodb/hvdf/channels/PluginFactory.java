package com.mongodb.hvdf.channels;

import java.util.HashMap;

import com.mongodb.hvdf.api.FrameworkError;
import com.mongodb.hvdf.api.ServiceException;
import com.mongodb.hvdf.configuration.PluginConfiguration;

public class PluginFactory {

	private static HashMap<String, String> registeredPlugins = 
			new HashMap<String, String>();
	
	static{
		registeredPlugins.put("batching", BatchingInterceptor.class.getName());
	}

	private static final String CONFIG_KEY = "config";
	private static final String CLASS_KEY = "class_name";

	public static ChannelInterceptor loadInterceptor(PluginConfiguration config) {
		
		// If the config item is not a document, return nothing
		String className = config.get(CLASS_KEY, String.class);
		if(registeredPlugins.containsKey(className)){
			className = registeredPlugins.get(className);
		}
		
		// Get the plugin instance
		PluginConfiguration interceptorConfig = config.get(CONFIG_KEY, PluginConfiguration.class);
		ChannelInterceptor plugin = createPlugin(ChannelInterceptor.class, className);
		plugin.configure(interceptorConfig.getRaw());
		
		return plugin;				
	}

	private static <T> T createPlugin(Class<T> pluginType, String className){
        
        T pluginInstance = null;

        try{
            // Find the plugin impl class and create instance
            Class<?> pluginClass = Class.forName(className);
            Object instance = pluginClass.newInstance();

            // Cast to the request plugin type
            pluginInstance = pluginType.cast(instance);

        } catch (ClassNotFoundException cnfex) {
            throw new ServiceException(FrameworkError.FAILED_TO_LOAD_PLUGIN_CLASS).
            set("class_name", className).set("plugin_type", pluginType.getName());
        } catch (ClassCastException cnfex) {
            throw new ServiceException(FrameworkError.PLUGIN_INCORRECT_TYPE).
            set("class_name", className).set("plugin_type", pluginType.getName());
        } catch (Exception e) {
            throw ServiceException.wrap(e, FrameworkError.PLUGIN_ERROR).
            set("class_name", className).set("plugin_type", pluginType.getName());
        }

        return pluginInstance;	
    }
	

}
