package com.mongodb.hvdf.api;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.mongodb.DBObject;
import com.mongodb.hvdf.util.JSONParam;

@JsonInclude(JsonInclude.Include.NON_NULL)
public class Source extends MongoDataObject {

	public static final String ID_KEY = "_id";
	public static final String DATA_KEY = "_d";
	
    public Source() {
        super();
    }

    public Source(DBObject sourceData) {
        super(sourceData);
    }

    public Source(String sourceId) {
        super();
        _dbObject.put(ID_KEY, sourceId);
    }

    public Source(String sourceId, JSONParam sourceData) {
        super();
        _dbObject.put(ID_KEY, sourceId);
        if(sourceData != null)
        	_dbObject.put(DATA_KEY, sourceData.toDBObject());
    }
    
    @JsonProperty("_id")
    public String getSourceId() {
        return (String)_dbObject.get(ID_KEY);
    }

    @JsonProperty("_id")
    public void setSourceId(String sourceId) {
        _dbObject.put(ID_KEY, sourceId);
    }

    @JsonProperty("_d")
    public DBObject getSourceData() {
        return (DBObject)_dbObject.get(DATA_KEY);
    }

    @JsonProperty("_d")
    public void setSourceData(JSONParam sourceData) {
        if(sourceData != null) {
            _dbObject.put(DATA_KEY,sourceData.toDBObject());
        }
    }
}
