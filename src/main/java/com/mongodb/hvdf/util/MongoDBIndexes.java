package com.mongodb.hvdf.util;

import java.util.ArrayList;
import java.util.List;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;

public class MongoDBIndexes {

	public static List<DBObject> getIndexesRegex(DB db, String nsRegex) {

		BasicDBObject query = new BasicDBObject();
		query.put("ns", new BasicDBObject("$regex", nsRegex));

        DBCursor cur = db.getCollection("system.indexes").find(query);

        List<DBObject> list = new ArrayList<DBObject>();

        while(cur.hasNext()) {
            list.add(cur.next());
        }

        return list;
	}	
}
