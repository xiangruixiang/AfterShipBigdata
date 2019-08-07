import java.io.FileOutputStream;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Properties;

public class test {

    static String GCPprojectId = "";
    static String GCPtopic = "";
    static String MongoDBServerIP = "";
    static String MongoDBServerPort = "";
    static String MongoDBDatabase = "";
    static String MongoDBTable = "";

    public static boolean isDateString(String datevalue, String dateFormat) {

        try {
            SimpleDateFormat fmt = new SimpleDateFormat(dateFormat);
            java.util.Date dd = fmt.parse(datevalue);
            if (datevalue.equals(fmt.format(dd))) {
                return true;
            } else {
                return false;
            }
        } catch (Exception e) {
            return false;
        }
    }


    public static void main(String args[]) throws ParseException, IOException {


        System.out.println(isDateString("2018-01-01","yyyy-MM-dd"));



      /*  Properties pro = new Properties();
        // 存储

        pro.setProperty("SearchTime", "2018-03-06 05:13:14");
        // 获取
        // String url1=pro.getProperty("url1", "test");//存在获取给定值，不存在获取默认值
        // System.out.println(url1);

        // 存储到e:others绝对路径 盘符：
        // pro.store(new FileOutputStream("e:/others/db.properties"), "db配置");
        // pro.storeToXML(new FileOutputStream("e:/others/db.xml"), "db配置");
        // 使用相对路径 当前工程
        try {
            pro.store(new FileOutputStream("/Users/xiangruixiang/Documents/db.properties"), "db配置");
        } catch (IOException e) {
            e.printStackTrace();
        }*/



/*
        System.out.println(args[0]);


        Properties resource = new Properties();
        FileInputStream stream = null;


        try {
            stream = new FileInputStream("/Users/xiangruixiang/Documents/db.properties");
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        resource.load(stream);

        //read properties

        System.out.println(resource.getProperty("SearchTime").toString());


    }
*/

/*
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss mmm");
        Date now = new Date();
        System.out.println("当前时间：" + sdf.format(now));

        Calendar nowTime = Calendar.getInstance();
        nowTime.add(Calendar.HOUR_OF_DAY, -12);
        System.out.println(sdf.format(nowTime.getTime()));


*/

/*
        SimpleDateFormat newsdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss mmm");

        Date newnow = newsdf.parse("2019-04-19 15:44:17 044");
        System.out.println("当前时间：" + newsdf.format(newnow));

        Calendar nowTime = Calendar.getInstance();
        nowTime.add(Calendar.HOUR_OF_DAY, -3);
        System.out.println(newsdf.format(nowTime.getTime()));
        */

/*

        SimpleDateFormat sdf=new SimpleDateFormat("yyyy-MM-dd HH:mm:ss mmm");
        String str="2019-04-19 06:44:17 044";
        Date dt=sdf.parse(str);
        Calendar rightNow = Calendar.getInstance();
        rightNow.setTime(dt);
        rightNow.add(Calendar.HOUR_OF_DAY,-8);//日期加10天
        Date dt1=rightNow.getTime();
        String reStr = sdf.format(dt1);
        System.out.println(reStr);

*/

/*
        Random r = new Random();
        for (int i = 0; i < 10; i++) {
            //int num = (int) (Math.random() *3 + 1);
            int a = r.nextInt(3) + 1;
            System.out.println("a:" + a);
           // System.out.println("b:" + num);
        }

     */

/*

        int randNumber = (int) ((Math.random() * 9 + 1) * 1000);
        System.out.println(randNumber);

*/

/*

        Stack<String> st = new Stack<String>();


        st.push("a");
        st.push("b");
        st.push("c");
        st.push("d");

        for(int i=0; i<4; i++){
            System.out.println(st.pop());
        }

*/


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

}

