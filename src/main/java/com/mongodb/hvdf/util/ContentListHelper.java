package com.mongodb.hvdf.util;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

import org.bson.types.ObjectId;

import com.mongodb.DBObject;
import com.mongodb.hvdf.api.Sample;

public class ContentListHelper {

    private static Comparator<Sample> backwardTimeComparator = new Comparator<Sample>() {
        @Override
        public int compare(Sample o1, Sample o2) {
            final ObjectId oid2 = (ObjectId)o2.getId();
            final ObjectId oid1 = (ObjectId)o1.getId();
            final int compare = oid2.compareTo(oid1);
            return compare;
        }};
                    
    private static Comparator<Sample> timeOrderComparator = new Comparator<Sample>() {
        @Override
        public int compare(Sample o1, Sample o2) {
            final ObjectId oid2 = (ObjectId)o2.getId();
            final ObjectId oid1 = (ObjectId)o1.getId();
            final int compare = oid1.compareTo(oid2);
            return compare;
        }};

    public static List<Sample> extractContent(
            final List<Sample> source, final DBObject anchor, 
            final int limit, final boolean allowFutureAnchor){
        int count = Math.abs(limit);
        final boolean forward = limit > 0;       
        List<Sample> results = new ArrayList<Sample>(count);
        
        if(anchor != null){                
            // find the anchor object, then walk backward or forward in time
            Sample markerObject = new Sample(anchor);
            int index = Collections.binarySearch(source, markerObject, timeOrderComparator);
            
            // if the anchor is past all values, it needs to be allowed
            if(Math.abs(index) < source.size() || allowFutureAnchor){
                if(forward == true){
                    index = index < 0 ? Math.abs(index) - 2 : Math.abs(index) - 1;
                    for(int i = index; i >= 0 && count-- > 0; --i){
                        results.add(source.get(i));
                    }                                        
                } else {
                    index = index < 0 ? Math.abs(index) - 1 : Math.abs(index) + 1;
                    for(int i = index; i < source.size() && count-- > 0; ++i){
                        results.add(source.get(i));
                    }
                    Collections.reverse(results);
                }
            }
        } else if(forward == true) {
            // just read from the end of the user content list 
            for(int i = source.size() - 1; i >= 0 && count-- > 0; --i)
                results.add(source.get(i));               
        }
    
        return results;
    }
    
    public static ListWalker<Sample> getContentWalker(
            final List<Sample> source, final DBObject anchor, final int limit) {
        
        ListWalker<Sample> walker = null;        
        if(anchor == null){ 
            // Use reverse list walker for each user content list
            return new ReverseListWalker<Sample>(source);
        } else {
            Sample markerObject = new Sample(anchor);
            int index = Collections.binarySearch(source, markerObject, timeOrderComparator);
            if(limit > 0){

                // Use reverse list walker anchored in each user content list
                index = index < 0 ? Math.abs(index) - 2 : Math.abs(index) - 1;
                walker = new ReverseListWalker<Sample>(source, index);
            } else {

                // Use forward list walker anchored in each user content list
                index = index < 0 ? Math.abs(index) - 1 : Math.abs(index) + 1;
                walker = new ListWalker<Sample>(source, index);
            }
        }
        
        return walker;
    }

    public static List<Sample> merge(final List<ListWalker<Sample>> walkers, final int limit) {
        int count = Math.abs(limit);
        boolean forward = limit > 0;       
        Comparator<Sample> comparator = forward == true ? 
                backwardTimeComparator : timeOrderComparator;
        List<Sample> result = new ArrayList<Sample>(count);
        ListWalker<Sample> lowest;

        while (result.size() < count) {
            lowest = null;
            for (ListWalker<Sample> l : walkers) {
                if (! l.atEnd()) {
                    if (lowest == null) {
                        lowest = l;
                    }
                    else if (l.get() != null && comparator.compare(l.get(), lowest.get()) <= 0) {
                        lowest = l;
                    }
                }
            }
            
            // Add the lowest value to the result or break if
            // all lists have been exhausted
            if(lowest != null)
                result.add(lowest.step());
            else
                break;
        }
        
        
        if(forward == false)
            Collections.reverse(result);
        
        return result;
    }
}
