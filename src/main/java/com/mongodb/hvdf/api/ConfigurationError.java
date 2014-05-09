package com.mongodb.hvdf.api;

import javax.ws.rs.core.Response.Status;

public enum ConfigurationError implements ServiceException.ErrorCode{
			
	INVALID_CONFIG_TYPE(5001),
	REQUIRED_CONFIG_MISSING(5002);
	
    private final Status response;
    private final int number;
    
    ConfigurationError(final Status responseStatus, final int errorNumber){
        this.response = responseStatus;
        this.number = errorNumber;
    }
    
    ConfigurationError(final int errorNumber){
        this(Status.FORBIDDEN, errorNumber);
    }
    
    @Override
    public int getErrorNumber(){
        return this.number;
    }
    
    @Override
    public Status getResponseStatus(){
        return this.response;
    }
}
	