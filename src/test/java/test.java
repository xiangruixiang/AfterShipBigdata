import org.apache.log4j.Logger;
import org.bson.Document;
import org.json.JSONObject;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.io.*;



public class test {

    static String GCPprojectId = "";
    static  String GCPtopic = "";
    static  String MongoDBServerIP = "";
    static  String MongoDBServerPort = "";
    static  String MongoDBDatabase = "";
    static  String MongoDBTable = "";


    public static void main( String args[] ) throws ParseException {



 /*       Logger log = Logger.getLogger(test.class.getClass());

        log.info("info");

        log.debug("debug");
        log.error("error");
        System.out.println("System.out.println");*/

/*

        if (args.length != 1) {
            System.err.println("缺少配置文件");
            System.exit(-1);
        }

        Properties resource = new Properties();
        FileInputStream stream = null;

        try {
            stream = new FileInputStream(args[0]);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        resource.load(stream);

        GCPprojectId = resource.getProperty("GCPprojectId");
        GCPtopic = resource.getProperty("GCPtopic");
        MongoDBServerIP = resource.getProperty("MongoDBServerIP");
        MongoDBServerPort = resource.getProperty("MongoDBServerPort");
        MongoDBDatabase = resource.getProperty("MongoDBDatabase");
        MongoDBTable = resource.getProperty("MongoDBTable");

        System.out.println(GCPprojectId);
        System.out.println(GCPtopic);
        System.out.println(MongoDBServerIP);
        System.out.println(MongoDBServerPort);
        System.out.println(MongoDBDatabase);
        System.out.println(MongoDBTable);
*/

/*
        String times= "Tue Mar 06 00:10:01 CST 2018";
        SimpleDateFormat USDateTime = new SimpleDateFormat("EEE MMM dd HH:mm:ss zzz yyyy", Locale.US);


        Date updatedTime = USDateTime.parse(times);
        String filterDateFrom = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(updatedTime);
        System.out.println(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss").format(updatedTime));

        SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        Timestamp filterDateFromTs = new Timestamp ((dateFormat.parse(filterDateFrom)).getTime());

        System.out.println(String.valueOf(filterDateFromTs.getTime()).substring(0,10));

        */

        String jsonString = "{\"name\":\"ma\",\"age\":null}";

        String aa;

        Map<String,Object> stu = new HashMap<String, Object>();

        JSONObject strjson = new JSONObject(jsonString);

        Object strAge = strjson.get("age");

        stu.put("name","aa");
        stu.put("age",strAge);

        JSONObject json = new JSONObject(stu);

        System.out.println(jsonString.toString());





    }
}
