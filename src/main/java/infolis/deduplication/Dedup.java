package infolis.deduplication;

import com.mongodb.BasicDBObject;
import com.mongodb.DB;
import com.mongodb.DBCollection;
import com.mongodb.DBCursor;
import com.mongodb.DBObject;
import com.mongodb.MongoClient;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 *
 * @author domi
 */
public class Dedup {

    public static void main(String args[]) throws UnknownHostException {

        MongoClient mongoClient = new MongoClient("localhost", 27018);
        DB db = mongoClient.getDB("infolis-web");
        DBCollection c = db.getCollection("entities");

        Map<String,List<DBObject>>  names = selectAll(c);
        for(String s : names.keySet()) {
            //TODO: some names have quotation marks, others do not have quotation marks but could be the same name
            //TODO: why are some names "string"? is this the "correct" name or is there some problem?
            System.out.println(s+"\t"+names.get(s).size());
        }
        System.out.println("-----");
        selectAllRecordByAttribute(c);

        //get all entities and save the possible names (across all datasets/tags?)
        //get the entities with the same name 
        //if a name occurs more than two times -> no duplicate, e.g. introduction
        //get the according authors
        //apply a similarity measure, e.g. allow abbreviations, first name, last name vs. last name, first name etc.
        //if a duplicate is found:
        //  create a new entity with the type, e.g. sameAs
        //  name the entity something like name_sameAs
        //  put all URIs of the duplicates as alternativeNames
        //get all entity links poiting to one of the duplicates
        //update the entity link by pointing to the group entity and not to the single entity anymore
        //TODO: different strategies for pubs/studies? filter for example by empty file (resp. studies do not have a file)?
    }

    private static void selectAllRecordByAttribute(DBCollection collection) {
        BasicDBObject whereQuery = new BasicDBObject();
        whereQuery.put("name", "string");
        DBCursor cursor = collection.find(whereQuery);
        while (cursor.hasNext()) {
            System.out.println(cursor.next());
        }
    }

    private static Map<String,List<DBObject>> selectAll(DBCollection collection) {
        DBCursor cursor = collection.find();
        Map<String,List<DBObject>> names = new HashMap<>();

        while (cursor.hasNext()) {
            DBObject o = cursor.next();
            if (o.keySet().contains("name")) {
                if(names.containsKey(o.get("name").toString())) {
                    names.get(o.get("name").toString()).add(o);
                }
                else {
                    List<DBObject> l = new ArrayList<>();
                    l.add(o);
                    names.put(o.get("name").toString(), l);
                }
            }
        }
        return names;
    }
}
