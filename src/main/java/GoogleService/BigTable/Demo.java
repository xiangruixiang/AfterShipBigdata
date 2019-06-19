package GoogleService.BigTable;

import com.alibaba.fastjson.JSON;
import com.google.cloud.bigtable.hbase.BigtableConfiguration;
import com.google.common.base.Strings;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.*;

import static java.lang.Character.UnicodeBlock.*;



/**
 *  此函数用于 查询big table 示例， 用于shipment 界面查询
 *
 */
public class Demo {

    // Refer to table metadata names by byte array in the HBase API
    private static final byte[] TABLE_NAME = Bytes.toBytes("eBay_checkpoints");
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf");


    public static void main(String[] args) {


        // Consult system properties to get project/instance
        String projectId = "aftership-team-data";
        String instanceId = "test-demo";
  /*
        String userid = "62ea48f64389525f31006338";
        String reverseUserid = new StringBuffer(userid).reverse().toString();
        String startRow = reverseUserid + "#20180300112343000";
        String endRow = reverseUserid + "#20180318182343000";
        Map<String,Object> colMap = new HashMap<String,Object>();
        String searchValue="";



        // insert data
        String Rowkey= reverseUserid + "#20180306111218";
        System.out.println(Rowkey);
        colMap.put("Date","20180306111218");
        colMap.put("Title","222211111");
        colMap.put("Tracking_number","aaaaaa");
        colMap.put("Customer_email","bbbbbb");
        colMap.put("Order_number","5@aftership.com");
        colMap.put("Customer_phone","342652w524");
        colMap.put("Origin","japan");
        colMap.put("Destination","USA");
        colMap.put("Courier","");
        colMap.put("Status","In Transit");
        colMap.put("Return_to_sender","FALSE");

        WriteTable(projectId, instanceId, Rowkey, colMap);

*/


/*
        //search data Filter
        //colMap.put("Courier","UPS");
        colMap.put("Destination","china");
        colMap.put("Origin","USA");
        // colMap.put("Return_to_sender","FALSE");
        FilterAndSearchValue(projectId, instanceId, "cf1",startRow,endRow,colMap);

*/



/*
        //search data search box
        String searchValue = "bbb";
        FilterAndSearchValue(projectId, instanceId, "cf1",startRow,endRow, searchValue);
*/


/*
        //search data Filter And  SearchBox
        Map<String,Object> searchMap = new HashMap<String,Object>();
        colMap.put("Courier","UPS");
        colMap.put("Destination","japan");
        colMap.put("Status","Out for Delivery");

        String searchValue = "5@aftership.com";

        FilterAndSearchValue(projectId, instanceId, "cf1", startRow, endRow, colMap, searchValue);


        //filters
        colMap.put("Courier","UPS");
        colMap.put("Destination","japan");
        colMap.put("Status","Out for Delivery");

        // search box
        searchValue = "5@";
        mutipleFilter(projectId, instanceId, "cf1", startRow, endRow, colMap, searchValue);
*/


        //delete table

        deleteTimeRange(projectId, instanceId);

    }


    public static void deleteTimeRange(String projectId, String instanceId) {

        String rowPrifix = "3ae7c#0#15202";
        try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

            // Retrieve the table we just created so we can do some reads and writes
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));
            Scan scan = new Scan();
            scan.setFilter(new PrefixFilter(rowPrifix.getBytes()));
            ResultScanner rs = table.getScanner(scan);

            List<Delete> list = getDeleteList(rs);
            if (list.size() > 0) {

                table.delete(list);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    private static List<Delete> getDeleteList(ResultScanner rs) {

        List<Delete> list = new ArrayList<>();
        try {

            for (Result r : rs) {
                Delete d = new Delete(r.getRow());
                list.add(d);
            }
        } finally {
            rs.close();
        }
        return list;
    }


    private static void mutipleFilter(String projectId, String instanceId, String colFamily, String startRow, String endRow, Map<String, Object> colMap, String searchValue) {

        Map<String,Object> searchMap = new HashMap<String,Object>();
        Map<String,Object> valueMap = new HashMap<String,Object>();
        Stack<String> dataList = new Stack<String>();

        String jsonStr = null;

        try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

            // Retrieve the table we just created so we can do some reads and writes
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            Scan scan = new Scan();
            List<Filter> colFilters = new ArrayList<Filter>();
            List<Filter> searchFilters = new ArrayList<Filter>();

            // search userid and create date value
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(endRow.getBytes());

            searchMap.put("Tracking_number",searchValue);
            searchMap.put("Title",searchValue);
            searchMap.put("Order_number",searchValue);
            searchMap.put("Customer_email",searchValue);
            searchMap.put("Customer_phone",searchValue);

            for (Map.Entry<String, Object> entry : colMap.entrySet()) {
                SingleColumnValueFilter colFilter = new SingleColumnValueFilter(
                        Bytes.toBytes(colFamily),
                        Bytes.toBytes(entry.getKey()),
                        CompareFilter.CompareOp.EQUAL,
                        Bytes.toBytes(entry.getValue().toString()));
                colFilter.setFilterIfMissing(true);
                colFilters.add(colFilter);
            }


            for (Map.Entry<String, Object> entry : searchMap.entrySet()) {
                SingleColumnValueFilter searchFilter = new SingleColumnValueFilter(
                        Bytes.toBytes(colFamily),
                        Bytes.toBytes(entry.getKey()),
                        CompareFilter.CompareOp.EQUAL,
                        new RegexStringComparator(entry.getValue().toString()+".*"));
                searchFilter.setFilterIfMissing(true);

                searchFilters.add(searchFilter);
            }


            // match filters
            FilterList  colFilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,colFilters);
            FilterList  searchFilterList = new FilterList(FilterList.Operator.MUST_PASS_ONE,searchFilters);
            colFilterList.addFilter(searchFilterList);

            scan.setFilter(colFilterList);

            // show columns
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Tracking_number"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Title"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Customer_email"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Order_number"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Customer_phone"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Status"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Return_to_sender"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Origin"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Destination"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Courier"));

            ResultScanner scanner = table.getScanner(scan);
            int scanRows=0;

            //output result
            for (Result r : scanner) {
                for (Cell cell : r.rawCells()) {
                    valueMap.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
                jsonStr = JSON.toJSONString(valueMap);   //covert map to string
                dataList.add(jsonStr);      //add to string
                valueMap.clear();
                scanRows=scanRows+1;
            }

            System.out.println("***************search result is ********************");
            System.out.println("");

            for (int row=0; row<scanRows; row++) {

                System.out.println(dataList.pop());

            }
            System.out.println("");
            System.out.println("***************search result is ********************");



        } catch (IOException e) {
            System.err.println("Exception while running HelloWorld: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }








    private static void FilterAndSearchValue(String projectId, String instanceId, String colFamily, String startRow, String endRow, Map<String, Object> colMap, String searchValue) {

        Map<String,Object> mapCol = new HashMap<String,Object>();

        List<String> dataList = new ArrayList<>();
        String jsonStr = null;

        try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

            // [START writing_rows]
            // Retrieve the table we just created so we can do some reads and writes
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            Scan scan = new Scan();

            List<Filter> colFilters = new ArrayList<Filter>();

            // search userid and create date value
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(endRow.getBytes());

            for (Map.Entry<String, Object> entry : colMap.entrySet()) {
                SingleColumnValueFilter colFilter = new SingleColumnValueFilter(
                        Bytes.toBytes(colFamily),
                        Bytes.toBytes(entry.getKey()),
                        CompareFilter.CompareOp.EQUAL,
                        Bytes.toBytes(entry.getValue().toString()));
                colFilter.setFilterIfMissing(true);
                colFilters.add(colFilter);
            }


            // page row filter
            Filter pageFilter = new PageFilter(100);
            colFilters.add(pageFilter);


            // match filters
            FilterList  colFilterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,colFilters);
            scan.setFilter(colFilterList);

            // show columns
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Tracking_number"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Title"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Customer_email"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Order_number"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Customer_phone"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Status"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Return_to_sender"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Origin"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Destination"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Courier"));

            ResultScanner scanner = table.getScanner(scan);

            //output result
            for (Result r : scanner) {
                for (Cell cell : r.rawCells()) {
                    String columnName = Bytes.toString(CellUtil.cloneQualifier(cell)).trim().toLowerCase();
                    String colValue =  Bytes.toString(CellUtil.cloneValue(cell));


                    if((columnName.equalsIgnoreCase("tracking_number") && colValue.startsWith(searchValue))
                        || (columnName.equalsIgnoreCase("title") && colValue.startsWith(searchValue))
                        || (columnName.equalsIgnoreCase("order_number") && colValue.startsWith(searchValue))
                        || (columnName.equalsIgnoreCase("customer_email") && colValue.startsWith(searchValue))
                        || (columnName.equalsIgnoreCase("customer_phone") && colValue.startsWith(searchValue))){

                        for (Cell outputCell : r.rawCells()) {
                            mapCol.put(Bytes.toString(CellUtil.cloneQualifier(outputCell)), Bytes.toString(CellUtil.cloneValue(outputCell)));
                        }
                        jsonStr = JSON.toJSONString(mapCol);   //covert map to string
                        dataList.add(jsonStr);      //add to string
                        mapCol.clear();
                        break;
                    }

                }

            }

            System.out.println("***************search result is ********************");
            System.out.println("");

            for (String message : dataList) {
                System.out.println(message);

            }
            System.out.println("");
            System.out.println("***************search result is ********************");



        } catch (IOException e) {
            System.err.println("Exception while running HelloWorld: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }











    private static void FilterAndSearchValue(String projectId, String instanceId, String colFamily, String startRow, String endRow, Map<String, Object> mapValues) {

        Map<String,Object> mapCol = new HashMap<String,Object>();
        List<String> dataList = new ArrayList<>();
        String jsonStr=null;

        try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

            // [START writing_rows]
            // Retrieve the table we just created so we can do some reads and writes
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            Scan scan = new Scan();

            List<Filter> filters = new ArrayList<Filter>();


            // search userid and create date value
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(endRow.getBytes());


            for (Map.Entry<String, Object> entry : mapValues.entrySet()) {

                SingleColumnValueFilter cusFilter = new SingleColumnValueFilter(
                        Bytes.toBytes(colFamily),
                        Bytes.toBytes(entry.getKey()),
                        CompareFilter.CompareOp.EQUAL,
                        Bytes.toBytes(entry.getValue().toString()));
                cusFilter.setFilterIfMissing(true);
                filters.add(cusFilter);
            }


            // page row filter
            Filter pageFilter = new PageFilter(100);
            filters.add(pageFilter);

            // match filters
            FilterList  filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,filters);

            // show columns
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Tracking_number"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Title"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Status"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Return_to_sender"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Origin"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Order_number"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Destination"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Customer_email"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Courier"));


            scan.setFilter(filterList);
            ResultScanner scanner = table.getScanner(scan);


            //output result
            for (Result r : scanner) {
                for (Cell cell : r.rawCells()) {
                    mapCol.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                  /*  System.out.println(
                            "Rowkey:"+Bytes.toString(r.getRow())+"  "+
                                    "Familiy:Quilifier:"+Bytes.toString(CellUtil.cloneQualifier(cell))+"  "+
                                    "Value:"+Bytes.toString(CellUtil.cloneValue(cell)));*/


                }
                jsonStr = JSON.toJSONString(mapCol);   //covert map to string
                //add to string
                mapCol.clear();
                dataList.add(jsonStr);
            }


            System.out.println("***************search result is ********************");
            System.out.println("");

            for (String message : dataList) {
                System.out.println(message);

            }
            System.out.println("");
            System.out.println("***************search result is ********************");


        } catch (IOException e) {
            System.err.println("Exception while running HelloWorld: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }




    private static void FilterAndSearchValue(String projectId, String instanceId, String colFamily, String startRow, String endRow, String searchValue) {

        Map<String,Object> mapCol = new HashMap<String,Object>();

        mapCol.put("Tracking_number",searchValue);
        mapCol.put("Title",searchValue);
        mapCol.put("Order_number",searchValue);
        mapCol.put("Customer_email",searchValue);
        mapCol.put("Customer_phone",searchValue);

        List<String> dataList = new ArrayList<>();
        String jsonStr=null;

        try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

            // [START writing_rows]
            // Retrieve the table we just created so we can do some reads and writes
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            Scan scan = new Scan();

            List<Filter> filters = new ArrayList<Filter>();

            // search userid and create date value
            scan.setStartRow(startRow.getBytes());
            scan.setStopRow(endRow.getBytes());


            for (Map.Entry<String, Object> entry : mapCol.entrySet()) {

                SingleColumnValueFilter cusFilter = new SingleColumnValueFilter(
                        Bytes.toBytes(colFamily),
                        Bytes.toBytes(entry.getKey()),
                        CompareFilter.CompareOp.EQUAL,
                        new RegexStringComparator(entry.getValue().toString()+".*"));
                cusFilter.setFilterIfMissing(true);
                filters.add(cusFilter);

            }


            // page row filter
            Filter pageFilter = new PageFilter(100);
            filters.add(pageFilter);


            // match filters
            FilterList  filterList = new FilterList(FilterList.Operator.MUST_PASS_ONE,filters);

            // show columns
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Tracking_number"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Title"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Status"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Return_to_sender"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Origin"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Order_number"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Destination"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Customer_email"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("Courier"));

            scan.setFilter(filterList);
            ResultScanner scanner = table.getScanner(scan);



            //output result
            for (Result r : scanner) {
                for (Cell cell : r.rawCells()) {
                    mapCol.put(Bytes.toString(CellUtil.cloneQualifier(cell)), Bytes.toString(CellUtil.cloneValue(cell)));
                }
                jsonStr = JSON.toJSONString(mapCol);   //covert map to string
                //add to string
                mapCol.clear();
                dataList.add(jsonStr);
            }

            System.out.println("***************search result is ********************");
            System.out.println("");

            for (String message : dataList) {
                System.out.println(message);

            }
            System.out.println("");
            System.out.println("***************search result is ********************");


        } catch (IOException e) {
            System.err.println("Exception while running HelloWorld: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }






    private static void WriteTable(String projectId, String instanceId, String rowKey,  Map<String, Object> mapValues) {

        try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

            // [START writing_rows]
            // Retrieve the table we just created so we can do some reads and writes
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));


            Put put = new Put(Bytes.toBytes(rowKey));
            for (Map.Entry<String, Object> entry : mapValues.entrySet()) {
                put.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes(entry.getKey()), Bytes.toBytes(entry.getValue().toString()));

            }
            table.put(put);


        } catch (IOException e) {
            System.err.println("Exception while running HelloWorld: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

    }




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
    }

}