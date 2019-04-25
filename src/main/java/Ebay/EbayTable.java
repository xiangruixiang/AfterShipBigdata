package Ebay;

import GoogleService.GooglePubSub.PublishMessage;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
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

import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.*;

public class EbayTable {

    static Logger log = Logger.getLogger(EbayTable.class.getClass());


    public void ReadMongoDB(String serverIP, String serverPort, String databaseName, String tableName, String searchTime){

        String sourceTimeBegin = null;
        String sourceTimeEnd;
        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//set date format
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'");

        Map<String,Object> mapSlug = new HashMap<String,Object>();

        BasicDBObject query = new BasicDBObject();


        try{
            //connect to mongodb
            MongoClient mongoClient = new MongoClient(serverIP, Integer.parseInt(serverPort));

            //connect to databases
            MongoDatabase mongoDatabase = mongoClient.getDatabase(databaseName);

            //choose tables
            MongoCollection<Document> eBayCollection = mongoDatabase.getCollection(tableName);
          //  MongoCollection<Document> trackingCollection = mongoDatabase.getCollection("trackings");

            // get couriers tables
            mapSlug = getSlugs(mongoDatabase, "couriers");

            Date searchTimeDate =df.parse(searchTime);   //convert to date

            while (true){

                log.info("use time is:" + searchTime);

                try {

                    searchTimeDate.setTime(searchTimeDate.getTime() + 1000); // add a second
                    searchTime = df.format(searchTimeDate);

                    sourceTimeBegin = searchTime.toString() + ".000Z";
                    sourceTimeEnd = searchTime.toString() + ".999Z";
                    Date startDate = sdf.parse(sourceTimeBegin);
                    Date endDate = sdf.parse(sourceTimeEnd);

                    //read tracking table
                    //readTrackingTable(searchTimeDate,searchTime, startDate,endDate, eBayCollection,query);

                    //read eBay table
                    readEbayTable(searchTimeDate,searchTime, startDate,endDate, eBayCollection,query, mapSlug);
                }
                catch (Exception e){
                    log.error( e.getClass().getName() + ": " + e.getMessage() );
                    log.error("Exception: Search mongo DB error");
                }
            }
        }catch(Exception e){
            System.err.println( e.getClass().getName() + ": " + e.getMessage() );
        }

    }



    //read tracking table
    public void readTrackingTable(Date searchTimeDate, String searchTime, Date startDate, Date endDate, MongoCollection<Document> collection, BasicDBObject query){

        String sourceTimeBegin = null;
        String sourceTimeEnd;
        String jsonStr;
        String filterDateFrom;
        Timestamp filterDateFromTs;
        long millionTime;
        int randNumber;
        Map<String,Object> mapMessage = new HashMap<String,Object>();
        List<String> dataList = new ArrayList<>();

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//set date format
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'");
        SimpleDateFormat USDateTime = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);


        try {

            query.put("updated_at", BasicDBObjectBuilder.start("$gte", startDate).add("$lt",endDate).get());

            log.info("Begin time is :" + df.format(new Date()));

            //execute query and return specified columns
            FindIterable<Document> findIterable = collection.find(query
            ).projection(Projections.include("_id", "user_id", "created_at", "updated_at", "origin_courier_id",
                    "destination_courier_id", "tag", "subtag", "delivery_time", "return_to_sender",  "origin_country_iso3",
                    "destination_country_iso3", "courier_origin_country_iso3", "courier_destination_country_iso3", "source",
                    "shipping_method", "checkpoints"));

            MongoCursor<Document> mongoCursor = findIterable.iterator();


            //loop output data
            while (mongoCursor.hasNext()) {
                Document json = mongoCursor.next();

                //add to map
                millionTime=System.currentTimeMillis();
                randNumber = (int) ((Math.random() * 9 + 1) * 1000);
                mapMessage.put("cus_id", Long.valueOf(String.valueOf(millionTime).concat(String.valueOf(randNumber))));
                mapMessage.put("id", json.get("_id").toString());
                mapMessage.put("user_id", json.get("user_id").toString());

                Date createdTime = USDateTime.parse(json.get("created_at").toString());
                filterDateFrom = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(createdTime);
                filterDateFromTs = new Timestamp ((df.parse(filterDateFrom)).getTime());
                mapMessage.put("created_at", String.valueOf(filterDateFromTs.getTime()).substring(0,10));

                Date updatedTime = USDateTime.parse(json.get("updated_at").toString());
                System.out.println(json.get("updated_at").toString());
                filterDateFrom = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(updatedTime);
                filterDateFromTs = new Timestamp ((df.parse(filterDateFrom)).getTime());
                mapMessage.put("updated_at", String.valueOf(filterDateFromTs.getTime()).substring(0,10));

                mapMessage.put("origin_courier_slug", JasonHandler(json, "origin_courier_id"));
                mapMessage.put("destination_courier_slug", JasonHandler(json, "destination_courier_id"));
                mapMessage.put("latest_status", json.get("tag").toString());
                mapMessage.put("latest_substatus", json.get("subtag").toString());
                mapMessage.put("days_in_transit", Integer.valueOf(JasonHandler(json, "delivery_time")));
                mapMessage.put("return_to_sender", Boolean.valueOf(JasonHandler(json, "return_to_sender")));
                mapMessage.put("user_origin_country", JasonHandler(json, "origin_country_iso3"));
                mapMessage.put("user_destination_country", JasonHandler(json, "destination_country_iso3"));
                mapMessage.put("courier_origin_country", JasonHandler(json, "courier_origin_country_iso3"));
                mapMessage.put("courier_destination_country", JasonHandler(json, "courier_destination_country_iso3"));
                mapMessage.put("source", JasonHandler(json, "source"));
                mapMessage.put("shipping_method", JasonHandler(json, "shipping_method"));

                JSONObject jsonObject = JSONObject.parseObject(json.toJson());
                JSONArray transidArray = jsonObject.getJSONArray("checkpoints");

                //get checkpoint tag:InTransit and checkpoint_time
                for (int i = 0; i < transidArray.size(); i++) {
                    JSONObject innerTransid = transidArray.getJSONObject(i);
                    // System.out.println("InTransit" + i  + ": " + innerTransid);
                    String inTransitTag = innerTransid.get("tag").toString();
                    if (inTransitTag.equalsIgnoreCase("InTransit")) {
                        mapMessage.put("in_transit_checkpoint_time", innerTransid.get("checkpoint_time").toString().substring(9,19));
                        break;
                    }
                }


                //get checkpoint tag:Delivered and checkpoint_time
                for (int i = 0; i < transidArray.size(); i++) {
                    JSONObject innerTransid = transidArray.getJSONObject(i);
                    // System.out.println("Delivered" + i  + ": " + innerTransid);
                    String deliveredStatus = innerTransid.get("tag").toString();
                    if (deliveredStatus.equalsIgnoreCase("Delivered")) {
                        mapMessage.put("delivered_checkpoint_time", innerTransid.get("checkpoint_time").toString().substring(9,19));
                        break;
                    }
                }

                jsonStr = JSON.toJSONString(mapMessage); //covert map to string
                mapMessage.clear();
                // log.info("origin value is:" + jsonObject.toString());
                log.info("output data is:" + jsonStr);

                dataList.add(jsonStr); //add message to list
            }
            //send to publish
            PublishMessage.publishMessagesWithErrorHandler(dataList, Mongodb.GCPprojectId, Mongodb.GCPTopicTrackingsToBigTableAndBigQuery);
        }
        catch (Exception e){
            log.error( e.getClass().getName() + ": " + e.getMessage() );
            log.error("Exception: Search mongo DB error");
        }
        finally {
            query.clear();  //clear search condition
            dataList.clear();   //clear data list
            log.info("End time is :" + df.format(new Date()));
        }

    }




    public void readEbayTable(Date searchTimeDate, String searchTime, Date startDate, Date endDate, MongoCollection<Document> collection, BasicDBObject query, Map<String, Object> mapSlug){
        String sourceTimeBegin = null;
        String sourceTimeEnd;
        String jsonStr;
        Map<String,Object> mapMessage = new HashMap<String,Object>();
        List<String> dataList = new ArrayList<>();

        SimpleDateFormat df = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");//set date format
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS'Z'");

        try {
            query.put("index_updated_at", BasicDBObjectBuilder.start("$gte", startDate).add("$lt",endDate).get());

            log.info("Begin time is :" + df.format(new Date()));

            //execute query and return specified columns
            FindIterable<Document> findIterable = collection.find(query
            ).projection(Projections.include("_id", "user_id","tracking_number", "custom_fields", "origin_courier_id",
                    "destination_courier_id", "origin_country_iso3", "destination_country_iso3", "source_info", "checkpoints"));

            MongoCursor<Document> mongoCursor = findIterable.iterator();

            //loop output data
            while (mongoCursor.hasNext()) {
                Document json = mongoCursor.next();
                JSONObject jsonObject = JSONObject.parseObject(json.toJson());
                JSONArray transidArray = jsonObject.getJSONArray("checkpoints");

                if(transidArray.size() > 0){
                    //add to map
                    mapMessage.put("substatus", JasonHandler(transidArray.getJSONObject(transidArray.size()-1).toString(),"subtag"));//  transidArray.getJSONObject(transidArray.size()-1).get("subtag"));
                    mapMessage.put("status", JasonHandler(transidArray.getJSONObject(transidArray.size()-1).toString(),"tag"));
                    mapMessage.put("tracking_number", json.get("tracking_number").toString());

                    if(json.containsKey("custom_fields")){

                        String valueObject = json.get("custom_fields").toString();

                        if(valueObject.contains("user_provided_carrier_name")) {
                            JSONObject customFieldsJson = JSONObject.parseObject(valueObject);
                            mapMessage.put("user_provided_carrier_name", JasonHandler(customFieldsJson.toString(), "user_provided_carrier_name"));
                        }
                        else {mapMessage.put("user_provided_carrier_name","null");}
                    }
                    else {
                        mapMessage.put("user_provided_carrier_name","null");
                    }

                    //slug 如果有tracking.destination_courier_id，根据destination_courier_id查找对应的slug。
                    // 如果没有，tracking.origin_courier_id => courier.slug。
                    String courierId = JasonHandler(json, "destination_courier_id").trim();
                    if(courierId.equals("null")) {
                        courierId = JasonHandler(json, "origin_courier_id").trim();
                    }


                    mapMessage.put("slug", "null");
                    for (Map.Entry<String, Object> entry : mapSlug.entrySet()) {
                        if( entry.getKey().trim().equals(courierId)){
                            mapMessage.put("slug", entry.getValue().toString());
                        }
                    }

                    mapMessage.put("message", transidArray.getJSONObject(transidArray.size()-1).get("message"));

                    SimpleDateFormat format =  new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
                    Long time = new Long(transidArray.getJSONObject(transidArray.size()-1).get("checkpoint_time").toString().substring(9,22));
                    Date dt=format.parse(format.format(time));
                    Calendar rightNow = Calendar.getInstance();
                    rightNow.setTime(dt);
                    rightNow.add(Calendar.HOUR_OF_DAY,-8);//日期加10天
                    Date dt1=rightNow.getTime();
                    String reStr = sdf.format(dt1);
                    mapMessage.put("date", reStr.substring(0,10));
                    mapMessage.put("time", reStr.substring(10,19).trim());

                    mapMessage.put("utc_offset", JasonHandler(transidArray.getJSONObject(transidArray.size()-1).toString(),"checkpoint_timezone"));
                    mapMessage.put("destination_country", JasonHandler(json, "destination_country_iso3"));
                    mapMessage.put("origin_country", JasonHandler(json, "origin_country_iso3"));
                    mapMessage.put("checkpoint_created_at", transidArray.getJSONObject(transidArray.size()-1).get("created_at").toString().substring(9,19));
                    mapMessage.put("tracking_id", json.get("_id").toString());
                    mapMessage.put("user_id", json.get("user_id").toString());
                    mapMessage.put("first_checkpoint_time",transidArray.getJSONObject(0).get("checkpoint_time").toString().substring(9,19));

                    //userid.6 # solt #  + created_at # tracking_id 10
                    String userId = json.get("user_id").toString().substring(19);
                    int nodeNumber = Integer.valueOf(transidArray.getJSONObject(transidArray.size()-1).get("created_at").toString().substring(9,19))%3;
                    String created_at = transidArray.getJSONObject(transidArray.size()-1).get("created_at").toString().substring(9,19);
                    String checkpointId = transidArray.getJSONObject(transidArray.size()-1).get("_id").toString().substring(20,33);

                    String rowKey = userId + "#" + nodeNumber + "#" + created_at + "#" + checkpointId;
                    mapMessage.put("rowKey", rowKey);

                    jsonStr = JSON.toJSONString(mapMessage); //covert map to string
                    mapMessage.clear();

                    //log.info("output data is:" + jsonStr);
                    dataList.add(jsonStr); //add message to list
                }
            }

            if(dataList.size()>0){
                //send to publish
             //   PublishMessage.publishMessagesWithErrorHandler(dataList, Mongodb.GCPprojectId, Mongodb.GCPeBayTopiceEbayToBigTable);
            }

        }
        catch (Exception e){
            log.error( e.getClass().getName() + ": " + e.getMessage() );
            log.error("Exception: Search mongo DB error");
        }
        finally {
            query.clear();  //clear search condition
            dataList.clear();   //clear data list
            log.info("End time is :" + df.format(new Date()));
        }

    }



    /*
    create date: 2019-04-18
    function: get courier tables id and slug
    parameters: mongoDatabase: mongo db object
                tableName: table name
     */
    public Map<String,Object> getSlugs(MongoDatabase mongoDatabase, String tableName ){

        Map<String,Object> mapSlug = new HashMap<String,Object>();

        //choose tables
        MongoCollection<Document> collection = mongoDatabase.getCollection(tableName);

        //execute query and return specified columns
        FindIterable<Document> findIterable = collection.find(
        ).projection(Projections.include("_id", "slug"));

        MongoCursor<Document> mongoCursor = findIterable.iterator();


        //loop output data
        while (mongoCursor.hasNext()) {
            Document json = mongoCursor.next();

                //add to map
                mapSlug.put(json.get("_id").toString().trim(), json.get("slug").toString().trim());

        }
                return mapSlug;
    }



    /*
    create date: 2019-03-08
    function: analyze json string
    parameters: jsonString: json string
                keyName: json node
     */

    public String JasonHandler(Document jsonString, String keyName) {

        String keyValues="";

        boolean keyObjeect = jsonString.containsKey(keyName);

        if(keyObjeect){

            Object valueObject = jsonString.get(keyName);

            if(valueObject == null){
                return "null";
            }

            else {
                keyValues = valueObject.toString();
            }
        }
        return keyValues;
    }


    public String JasonHandler(String jsonString, String keyName) {

        String keyValues="";
        JSONObject jsonObject = JSONObject.parseObject(jsonString);

        boolean keyObjeect = jsonObject.containsKey(keyName);

        if(keyObjeect){

            Object valueObject = jsonObject.get(keyName);

            if(valueObject == null){
                return "null";
            }
            else {
                keyValues = valueObject.toString();
            }
        }
        return keyValues;
    }
}
