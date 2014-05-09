package com.mongodb.hvdf.api;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.DBObject;
import com.mongodb.hvdf.util.JSONParam;

import org.bson.types.ObjectId;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Sample extends MongoDataObject {

	public static final String ID_KEY = "_id";
	public static final String SOURCE_KEY = "source";
	public static final String TS_KEY = "ts";
	public static final String DATA_KEY = "data";
	
    public Sample(final DBObject obj) {
        super(obj);
    }

    public Sample(final Source source, 
    		final long timeStamp, final JSONParam data) {
        super();
        _dbObject.put(ID_KEY, new ObjectId());
        _dbObject.put(SOURCE_KEY, source.getSourceId());
        
        _dbObject.put(TS_KEY, timeStamp != 0 ? timeStamp : System.currentTimeMillis());        	
        if(data != null)
        	_dbObject.put(DATA_KEY, data.toDBObject());
        
    }

    @JsonIgnore
    public Object getId() {
        return _dbObject.get(ID_KEY);
    }

    @JsonProperty("_id")
    public String getIdAsString() {
        return _dbObject.get("_id").toString();
    }

    @JsonProperty("ts")
    public long getTimeStamp() {
        return (Long) _dbObject.get(TS_KEY);
    }

    @JsonProperty("source")
    public String getSourceId() {
        return (String) _dbObject.get(SOURCE_KEY);
    }

    @JsonProperty("data")
    public DBObject getData() {
        return (DBObject) _dbObject.get(DATA_KEY);
    }
}
