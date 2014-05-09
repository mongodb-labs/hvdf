package com.mongodb.hvdf.configuration;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.hvdf.api.ConfigurationError;
import com.mongodb.hvdf.api.ServiceException;

public class PluginConfiguration {
	
	private static HashSet<Class<?>> registeredClasses = new HashSet<Class<?>>();
	
	static{
		registeredClasses.add(TimePeriod.class);
	}
	
	private final DBObject rawDocument;
	private final Class<?> targetClass;
	
	public PluginConfiguration(DBObject rawConfig, Class<?> target){
		
		if(rawConfig == null){
			rawConfig = new BasicDBObject();
		}
		
		this.rawDocument = rawConfig;
		this.targetClass = target;
	}

	
	public <T> T get(String itemName, Class<T> expectedType, T defaultValue) {
		
		Object itemObj = rawDocument.get(itemName);
		
		// If it exists, parse the object
		if(itemObj != null){
			return fromObject(itemName, expectedType, itemObj);
		}
		
		return defaultValue;	
	}
	
	public <T> T get(String itemName, Class<T> expectedType) {

		// If there is a valid config value then return it
		T value = get(itemName, expectedType, null);
		if(value != null) return value;

		// If not, throw a service exception
		throw new ServiceException("Required config item missing", 
				ConfigurationError.REQUIRED_CONFIG_MISSING).
					set("configuring", this.getClass()).
					set("requiredItem", itemName);	
	}	

	private <T> T fromObject(String itemName, Class<T> expectedType, Object itemObj) {
		
		try{			
			if(expectedType.equals(PluginConfiguration.class)){
				// This is an embedded config document
				return (T) (new PluginConfiguration((DBObject) itemObj, this.targetClass));
			}
			else if(registeredClasses.contains(expectedType)){
				// One of the specially handled types, construct
				try {
					return (expectedType.getConstructor(Object.class).newInstance(itemObj));
				} catch (Exception ex) {throw new ClassCastException();} 
			}
			else{
				// This must be directly convertible to target type
				return (T) itemObj;
			}
		} 
		catch(ClassCastException ccex){
			throw new ServiceException("Illegal type for config item", 
					ConfigurationError.INVALID_CONFIG_TYPE).
						set("configuring", targetClass).
						set(itemName, itemObj).
						set("expectedType", expectedType);	
		}				
	}

	public <T> List<T> getList(String itemName, Class<T> listOf) {
		
		List<T> list = new ArrayList<T>();
		Object itemObj = rawDocument.get(itemName);

		if(itemObj != null){
			
			// Must be a list
			if(itemObj instanceof BasicDBList){
				BasicDBList rawList = (BasicDBList)itemObj;
				for(Object listEntry : rawList){
					list.add(fromObject(itemName, listOf, listEntry));
				}
			}
			else{
				throw new ServiceException("Config item is not List", 
						ConfigurationError.INVALID_CONFIG_TYPE).
							set("configuring", targetClass).
							set(itemName, itemObj).
							set("expectedListOf", listOf);	
			}
		}
		
		// If the item doesnt exist
		return list;	
	}


	public DBObject getRaw() {
		return this.rawDocument;
	}
}
