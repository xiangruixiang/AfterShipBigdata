package Ebay;
/**
此函数用于读取配置文件中读所需要参数
        GCPprojectId： GCP 项目ID
        GCPTopicTrackingsToBigTableAndBigQuery : GCP pubsub 用于bigquery topic 名称
        GCPeBayTopiceEbayToBigTable：GCP pubsub bigtable topic 名称
        MongoDBServerIP ：mongo db 服务器地址
        MongoDBServerPort ：mongo db 服务器端口地址
        MongoDBDatabase ：mongo db 数据库名称
        MongoDBTable： mongo db 表名
        searchTime： 初始化的读取时间

*/
import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;


public class Mongodb {

    static String GCPprojectId = "";
    static  String GCPTopicTrackingsToBigTableAndBigQuery = "";
    static  String GCPeBayTopiceEbayToBigTable = "";
    static  String MongoDBServerIP = "";
    static  String MongoDBServerPort = "";
    static  String MongoDBDatabase = "";
    static  String MongoDBTable = "";
    static  String searchTime = "";
    static Logger log = Logger.getLogger(Mongodb.class.getClass());

    public static void main( String args[] ) throws IOException {

        if (args.length < 1) {
            log.error("Please input configuration file");
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

        //read properties
        GCPprojectId = resource.getProperty("GCPprojectId");
        GCPTopicTrackingsToBigTableAndBigQuery = resource.getProperty("GCPTopicTrackingsToBigTableAndBigQuery");
        GCPeBayTopiceEbayToBigTable = resource.getProperty("GCPeBayTopiceEbayToBigTable");
        MongoDBServerIP = resource.getProperty("MongoDBServerIP");
        MongoDBServerPort = resource.getProperty("MongoDBServerPort");
        MongoDBDatabase = resource.getProperty("MongoDBDatabase");
        MongoDBTable = resource.getProperty("MongoDBTable");

        // initialize time to begin search , please use updated_at value, example: 2018-03-06 00:10:00
        if(args.length>1){
            searchTime = args[1];
        }
        else {
            searchTime = resource.getProperty("SearchTime");
        }


        ReadEbayTable mongoDB = new ReadEbayTable();

        //read mongo db
        mongoDB.ReadMongoDB(MongoDBServerIP,  MongoDBServerPort, MongoDBDatabase, MongoDBTable, searchTime);

    }

}
