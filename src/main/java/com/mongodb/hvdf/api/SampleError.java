package com.mongodb.hvdf.api;

import javax.ws.rs.core.Response.Status;

public enum SampleError implements ServiceException.ErrorCode{
			
    INVALID_SAMPLE(Status.FORBIDDEN, 2001),
    SAMPLE_NOT_FOUND(Status.NOT_FOUND, 2002);
	
    private final Status response;
    private final int number;
    
    SampleError(final Status responseStatus, final int errorNumber){
        this.response = responseStatus;
        this.number = errorNumber;
    }
    
    SampleError(final int errorNumber){
        this(Status.INTERNAL_SERVER_ERROR, errorNumber);
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
	