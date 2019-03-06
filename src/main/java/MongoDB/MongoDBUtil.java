package MongoDB;

import GoogleService.GooglePubSub.PublishMessage;
import com.alibaba.fastjson.JSON;
import com.mongodb.BasicDBObject;
import com.mongodb.BasicDBObjectBuilder;
import com.mongodb.MongoClient;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoCursor;
import com.mongodb.client.MongoDatabase;
import com.mongodb.client.model.Projections;
import org.apache.log4j.Logger;
import org.bson.Document;

import java.text.SimpleDateFormat;
import java.util.*;

public class MongoDBUtil {

    static Logger log = Logger.getLogger(MongoDBUtil.class.getClass());

    public void ReadMongoDB(String serverIP, String serverPort, String databaseName, String tableName, String searchTime){

        String sourceTimeBegin="";
        String sourceTimeEnd="";
        String jsonStr="";
        long millionTime;
        int randNumber;

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//set date format
        SimpleDateFormat timer = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//set date format
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'");

        Map<String,Object> mapMessage = new HashMap<String,Object>();

        BasicDBObject query = new BasicDBObject();

        List<String> dataList = new ArrayList<>();

        SimpleDateFormat USDateTime = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);

        try{
            //connect to mongodb  104.196.146.203
            MongoClient mongoClient = new MongoClient(serverIP, Integer.parseInt(serverPort));

            //connect to databases
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);

            //choose tables
            MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

            Date searchTimeDate =df.parse(searchTime);   //convert to date


            while (true){

                log.info("use time is:" + sourceTimeBegin);

                try {

                    searchTimeDate.setTime(searchTimeDate.getTime() + 1000); // add a second
                    searchTime = df.format(searchTimeDate);

                    sourceTimeBegin = searchTime.toString() + ".000Z";
                    sourceTimeEnd = searchTime.toString() + ".999Z";
                    Date startDate = sdf.parse(sourceTimeBegin);
                    Date endDate = sdf.parse(sourceTimeEnd);

                    query.put("updated_at", BasicDBObjectBuilder.start("$gte", startDate).add("$lt",endDate).get());

                    log.info("Search begin time is :" + timer.format(new Date()));

                    //execute query and return specified columns
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
                        mapMessage.put("created_date", new SimpleDateFormat("yyyy-MM-dd").format(createdTime));

                        jsonStr = JSON.toJSONString(mapMessage); //covert map to string
                        mapMessage.clear();

                        log.info("output data is：" + jsonStr);

                        dataList.add(jsonStr); //add message to list
                    }
                    //send to publish
                    PublishMessage.publishMessagesWithErrorHandler(dataList, MongodbCRUD.GCPprojectId, MongodbCRUD.GCPprojectId);
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
