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

import static java.lang.Character.UnicodeBlock.*;
import com.google.common.base.Strings;

public class test {

    static String GCPprojectId = "";
    static  String GCPtopic = "";
    static  String MongoDBServerIP = "";
    static  String MongoDBServerPort = "";
    static  String MongoDBDatabase = "";
    static  String MongoDBTable = "";


    public static void main( String args[] ) throws ParseException {



        Stack<String> st = new Stack<String>();


        st.push("a");
        st.push("b");
        st.push("c");
        st.push("d");



        for(int i=0; i<4; i++){

            System.out.println(st.pop());

        }



        /*for(int i=1; i<5; i++){

            for(int j=1; j<=50; j++){

                if(j > 10){
                    break;
                }
                System.out.println("output:" + j);

            }

        }
*/


/*
        long keyDate = Long.valueOf("2018030511000000");

        keyDate = keyDate + 1;

        System.out.println(keyDate);
*/


     /*   Map<String,Object> colMap = new HashMap<String,Object>();
        colMap.put("title","test time");
        colMap.put("email","3@aftership.com");

        System.out.println(colMap.size());*/



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

/*

        String containChinese = "test,我有中文";
        String containNoChiese = "test, i don't contain chinese";
        System.out.println("containChinese 是否包含中文 :" + checkStringContainChinese(containChinese));
        System.out.println("containChinese 是否包含中文 :" + checkStringContainChinese(containNoChiese));
*/


    }
/*
    private static boolean checkStringContainChinese(String checkStr){
        if(!Strings.isNullOrEmpty(checkStr)){
            char[] checkChars = checkStr.toCharArray();
            for(int i = 0; i < checkChars.length; i++){
                char checkChar = checkChars[i];
                if(checkCharContainChinese(checkChar)){
                    return true;
                }
            }
        }
        return false;
    }

    private static boolean checkCharContainChinese(char checkChar){
        Character.UnicodeBlock ub = Character.UnicodeBlock.of(checkChar);
        if(CJK_UNIFIED_IDEOGRAPHS == ub || CJK_COMPATIBILITY_IDEOGRAPHS == ub || CJK_COMPATIBILITY_FORMS == ub ||
                CJK_RADICALS_SUPPLEMENT == ub || CJK_UNIFIED_IDEOGRAPHS_EXTENSION_A == ub || CJK_UNIFIED_IDEOGRAPHS_EXTENSION_B == ub){
            return true;
        }
        return false;
    }*/

}
