package com.mongodb.hvdf.util;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.mongodb.BasicDBObject;
import com.mongodb.DBObject;
import com.mongodb.util.JSON;
import com.mongodb.hvdf.api.FrameworkError;
import com.mongodb.hvdf.api.ServiceException;

import java.util.Map;

public class JSONParam {
	
    private DBObject dbObject = null;

    public JSONParam(String json){
        try {
        	dbObject = (DBObject) JSON.parse(json);
        }
        catch (Exception ex) {
            throw ServiceException.wrap(ex, FrameworkError.CANNOT_PARSE_JSON).
            	set("json", json);
        }
    }

    @JsonCreator
    public JSONParam(Map<String,Object> props) {
        dbObject = new BasicDBObject();
        dbObject.putAll(props);
    }

	@Override
	public boolean equals(Object obj) {
        if (obj == null)
            return false;
        if (obj == this)
            return true;
        if (!(obj instanceof JSONParam))
            return false;

        JSONParam rhs = (JSONParam) obj;
        return this.dbObject.equals(rhs.dbObject);
	}

	@Override
	public int hashCode() {
		return dbObject.hashCode();
	}

	@Override
	public String toString() {
		return dbObject.toString();
	}

	public DBObject toDBObject() {
        return dbObject;
    }
}
