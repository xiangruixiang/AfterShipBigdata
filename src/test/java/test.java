import org.apache.log4j.Logger;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.util.Properties;
import java.util.ResourceBundle;
import java.io.*;



public class test {

    static String GCPprojectId = "";
    static  String GCPtopic = "";
    static  String MongoDBServerIP = "";
    static  String MongoDBServerPort = "";
    static  String MongoDBDatabase = "";
    static  String MongoDBTable = "";


    public static void main( String args[] ) throws IOException {



 /*       Logger log = Logger.getLogger(test.class.getClass());

        log.info("info");

        log.debug("debug");
        log.error("error");
        System.out.println("System.out.println");*/


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







    }
}
