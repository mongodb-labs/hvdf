package com.mongodb.hvdf.allocators;

import java.util.Comparator;

public class SliceDetails {

	public String name;
	public long minTime;
	public long maxTime;
	
	public SliceDetails(String name, long minTime, long maxTime) {
		
		this.name = name;
		this.maxTime = maxTime;
		this.minTime = minTime;
	}
	
    public static class Comparators {

        public static Comparator<SliceDetails> MIN_TIME_DESCENDING = new Comparator<SliceDetails>() {
            @Override
            public int compare(SliceDetails o1, SliceDetails o2) {
                return Long.compare(o2.minTime, o1.minTime);
            }
        };
    }
}
