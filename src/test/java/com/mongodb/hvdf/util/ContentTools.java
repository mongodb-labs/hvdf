package com.mongodb.hvdf.util;

import java.util.concurrent.atomic.AtomicInteger;

import org.bson.types.ObjectId;

import com.mongodb.hvdf.api.Sample;
import com.mongodb.hvdf.api.Source;

public class ContentTools {
    
    private static AtomicInteger idSequence = new AtomicInteger();
    
    public static void implantSequentialId(Sample sample){
        ObjectId fakeId = new ObjectId(idSequence.getAndIncrement(), 0, 0);
        sample.toDBObject().put(Sample.ID_KEY, fakeId);       
    }

    public static Sample createSequentialSample(Source source){
        int postId = idSequence.getAndIncrement();
        Sample newPost = new Sample(source, postId, null);
        ObjectId fakeId = new ObjectId(postId, 0, 0);
        newPost.toDBObject().put(Sample.ID_KEY, fakeId);   
        
        return newPost;
    }

}
