package com.mongodb.hvdf.channels;


public abstract class ChannelInterceptor 
	extends ChannelPlugin 
	implements ChannelProcessor{
	
	protected ChannelProcessor next;

	protected void setNext(ChannelProcessor next){
		this.next = next;
	}	
}
