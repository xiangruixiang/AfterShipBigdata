package ReadMongoDB;

import GoogleService.GooglePubSub.PublishMessage;
import com.alibaba.fastjson.JSON;

import com.mongodb.*;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.bson.Document;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import java.text.SimpleDateFormat;
import java.util.*;



public class MongodbCRUD {

    static final String GCPprojectId = "aftership-team-data";
    static final String GCPtopic = "bigquery";

    public static void main( String args[] ){

        String searchTime;

        try{
            //connect to mongodb  104.196.146.203
            MongoClient mongoClient = new MongoClient( "104.196.146.203" , 27017 );

            //connect to databases
            MongoDatabase mongoDatabase = mongoClient.getDatabase("aftership_archived");

            //choose tables
            MongoCollection<Document> collection = mongoDatabase.getCollection("trackings_2018_03");


            searchTime = "2018-03-06 00:10:00";  //init search date
            SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//set date format
            SimpleDateFormat timer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//set date format
            Date date =df.parse(searchTime);   //convert to date

            Map<String,Object> mapMessage = new HashMap<String,Object>();
            String sourceTimeBegin="";
            String sourceTimeEnd="";
            SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'");
            BasicDBObject query = new BasicDBObject();
            List<String> dataList = new ArrayList<>();
            String jsonStr="";

            SimpleDateFormat USDateTime = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);
            long millionTime;
            int randNumber;

            while (true){

                date.setTime(date.getTime() + 1000);
                searchTime = df.format(date);

                sourceTimeBegin = searchTime.toString() + ".000Z";
                sourceTimeEnd = searchTime.toString() + ".999Z";

                System.out.println("use time is:" + sourceTimeBegin);

                // add query conditions
              /*  Pattern pattern = Pattern.compile("^.*" + searchTime + ".*$", Pattern.CASE_INSENSITIVE);
                BasicDBObject query = new BasicDBObject();
                query.put("updated_at", pattern);  2018-03-31 00:03:34.360Z  2018-03-31 02:00:00.000Z
*/
                try {
                    Date startDate = sdf.parse(sourceTimeBegin);
                    Date endDate = sdf.parse(sourceTimeEnd);

                    query.put("updated_at", BasicDBObjectBuilder.start("$gte", startDate).add("$lt",endDate).get());

                    System.out.println("Search begin time is :" + timer.format(new Date()));

                    //execute query
                    FindIterable<Document> findIterable = collection.find(query
                        ).projection(Projections.include( "tracking_number", "user_id", "updated_at", "created_at",
                                "subtag", "tag", "_id"));

                    MongoCursor<Document> mongoCursor = findIterable.iterator();


                    //loop output data
                    while (mongoCursor.hasNext()) {
                        Document json = mongoCursor.next();

                        //add to map
                        millionTime=System.currentTimeMillis();
                        randNumber = (int) ((Math.random() * 9 + 1) * 1000);
                        mapMessage.put("cus_id", Long.valueOf(String.valueOf(millionTime).concat(String.valueOf(randNumber))));
                        mapMessage.put("id", json.get("_id").toString());
                        mapMessage.put("tracking_number", json.getString("tracking_number"));
                        mapMessage.put("user_id", json.get("user_id").toString());
                        Date updatedTime = USDateTime.parse(json.get("updated_at").toString());
                        mapMessage.put("updated_at", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(updatedTime));
                        Date createdTime = USDateTime.parse(json.get("created_at").toString());
                        mapMessage.put("created_at", new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(createdTime));
                        mapMessage.put("subtag", json.get("subtag").toString());
                        mapMessage.put("tag", json.get("tag").toString());
                        // mapMessage.put("insert_time",System.currentTimeMillis()/1000);
                        mapMessage.put("created_date", new SimpleDateFormat("yyyy-MM-dd").format(createdTime));


                        jsonStr = JSON.toJSONString(mapMessage); //covert map to string
                        mapMessage.clear();

                        System.out.println("output data is：" + jsonStr);

                        dataList.add(jsonStr); //add to list
                    }
                    //send to publish
                    PublishMessage.publishMessagesWithErrorHandler(dataList, GCPprojectId, GCPtopic);
                }
                catch (Exception e){
                    System.err.println( e.getClass().getName() + ": " + e.getMessage() );
                    System.out.println("Exception: Search mongo DB error");
                }
                finally {
                    query.clear();  //clear search condition
                    dataList.clear();   //clear data list
                    System.out.println("Search end time is :" + timer.format(new Date()));
                }
            }
        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }
    }






}
