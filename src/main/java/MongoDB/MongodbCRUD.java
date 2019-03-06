package MongoDB;

import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;


public class MongodbCRUD {

    static String GCPprojectId = "";
    static  String GCPtopic = "";
    static  String MongoDBServerIP = "";
    static  String MongoDBServerPort = "";
    static  String MongoDBDatabase = "";
    static  String MongoDBTable = "";
    static  String searchTime = "";
    static Logger log = Logger.getLogger(MongodbCRUD.class.getClass());

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
        GCPtopic = resource.getProperty("GCPtopic");
        MongoDBServerIP = resource.getProperty("MongoDBServerIP");
        MongoDBServerPort = resource.getProperty("MongoDBServerPort");
        MongoDBDatabase = resource.getProperty("MongoDBDatabase");
        MongoDBTable = resource.getProperty("MongoDBTable");

        // initialize time to begin search , please use updated_at value, example: 2018-03-06 00:10:00
        searchTime = resource.getProperty("SearchTime");

        MongoDBUtil mongoDB = new MongoDBUtil();

        //read mongo db
        mongoDB.ReadMongoDB(MongoDBServerIP,  MongoDBServerPort, MongoDBDatabase, MongoDBTable ,searchTime);

    }

}
