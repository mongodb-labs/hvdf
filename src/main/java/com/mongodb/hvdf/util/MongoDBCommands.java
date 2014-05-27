package com.mongodb.hvdf.util;

import com.mongodb.BasicDBList;
import com.mongodb.BasicDBObject;
import com.mongodb.CommandResult;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBObject;

public class MongoDBCommands {
	
	public static void update(DBCollection coll, DBObject query, 
			DBObject data, boolean upsert, boolean multi){

		// build the update command info
		BasicDBObject update = new BasicDBObject("q", query);
		update.append("u", data).append("upsert", upsert).append("multi", multi);
		
		// the command requires a list of operations
		BasicDBList updates = new BasicDBList();
		updates.add(update);
		
		// create the command itself
		BasicDBObject updateCmd = new BasicDBObject("update", coll.getName());
		updateCmd.append("updates", updates);
		
		// execute the command on the collections database
		DB configDb = coll.getDB();
		CommandResult result = configDb.command(updateCmd);
		
		// convert any error to an exception and throw
		result.throwOnError();

	}

}
