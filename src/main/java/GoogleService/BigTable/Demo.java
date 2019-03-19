package GoogleService.BigTable;

import com.google.cloud.bigtable.hbase.BigtableConfiguration;

import org.apache.hadoop.hbase.*;
import org.apache.hadoop.hbase.client.Admin;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.filter.*;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * A minimal application that connects to Cloud Bigtable using the native HBase API
 * and performs some basic operations.
 */
public class Demo {

    // Refer to table metadata names by byte array in the HBase API
    private static final byte[] TABLE_NAME = Bytes.toBytes("my-table");
    private static final byte[] COLUMN_FAMILY_NAME = Bytes.toBytes("cf1");



    public static void main(String[] args) {
        // Consult system properties to get project/instance
        String projectId = "aftership-team-data";
        String instanceId = "test-demo";

        ReadTable(projectId, instanceId);
        //WriteTable(projectId, instanceId);
    }

    private static void ReadTable(String projectId, String instanceId) {

        try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

            // [START writing_rows]
            // Retrieve the table we just created so we can do some reads and writes
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

            Scan scan = new Scan();

            List<Filter> filters = new ArrayList<Filter>();
/*

            Filter filterLess = new RowFilter(
                    CompareFilter.CompareOp.EQUAL,
                    new BinaryComparator(Bytes.toBytes("83360013f52598346f84ae26#20180310112343003")));
            filters.add(filterLess);


            //row key filter
            Filter filterPrefix = new PrefixFilter(Bytes.toBytes("83360013f52598346f84ae26"));
            filters.add(filterPrefix);
*/

            // search userid and create date value
            scan.setStartRow("83360013f52598346f84ae26#20180306112343000".getBytes());
            scan.setStopRow("83360013f52598346f84ae26#20180313112343005".getBytes());


/*
            //column filter
            SingleColumnValueFilter titleFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("cf1"),
                    Bytes.toBytes("title"),
                    CompareFilter.CompareOp.EQUAL,Bytes.toBytes( "test222"));
            titleFilter.setFilterIfMissing(true);
            filters.add(titleFilter);
*/


/*

            //column filter
            SingleColumnValueFilter emailFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("cf1"),
                    Bytes.toBytes("email"),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes( "3@aftership.com"));
            emailFilter.setFilterIfMissing(true);
            filters.add(emailFilter);


            //column filter
            SingleColumnValueFilter tracking_idFilter = new SingleColumnValueFilter(
                    Bytes.toBytes("cf1"),
                    Bytes.toBytes("tracking_id"),
                    CompareFilter.CompareOp.GREATER_OR_EQUAL,Bytes.toBytes( "444"));
            tracking_idFilter.setFilterIfMissing(true);
            filters.add(tracking_idFilter);

*/

            // page row filter
            Filter pageFilter = new PageFilter(100);
            filters.add(pageFilter);


            // match filters
            FilterList  filterList = new FilterList(FilterList.Operator.MUST_PASS_ALL,filters);

            // show columns
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("created_at"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("title"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("email"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("tracking_id"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("user_id"));
            scan.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("city"));

            scan.setFilter(filterList);
            ResultScanner scanner = table.getScanner(scan);

            //output result
            for (Result r : scanner) {
                for (Cell cell : r.rawCells()) {
                    System.out.println(
                            "Rowkey:"+Bytes.toString(r.getRow())+"  "+
                                    "Familiy:Quilifier:"+Bytes.toString(CellUtil.cloneQualifier(cell))+"  "+
                                    "Value:"+Bytes.toString(CellUtil.cloneValue(cell)));
                }
            }


        } catch (IOException e) {
            System.err.println("Exception while running HelloWorld: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }


    private static void WriteTable(String projectId, String instanceId) {

        try (Connection connection = BigtableConfiguration.connect(projectId, instanceId)) {

            // [START writing_rows]
            // Retrieve the table we just created so we can do some reads and writes
            Table table = connection.getTable(TableName.valueOf(TABLE_NAME));

                // Each row has a unique row key.
                //
                // Note: This example uses sequential numeric IDs for simplicity, but
                // this can result in poor performance in a production application.
                // Since rows are stored in sorted order by key, sequential keys can
                // result in poor distribution of operations across nodes.
                //
                // For more information about how to design a Bigtable schema for the
                // best performance, see the documentation:
                //
                //     https://cloud.google.com/bigtable/docs/schema-design
                String rowKey = "83360013f52598346f84ae26#20180106#6a97cbfcf47909d40b586a34";

                // Put a single row into the table. We could also pass a list of Puts to write a batch.
                Put put = new Put(Bytes.toBytes(rowKey));
                put.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes("created_at"), Bytes.toBytes("20180314"));
                put.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes("email"), Bytes.toBytes("6@aftership.com"));
                put.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes("title"), Bytes.toBytes("test666"));
                put.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes("tracking_id"), Bytes.toBytes("666"));
                put.addColumn(COLUMN_FAMILY_NAME, Bytes.toBytes("user_id"), Bytes.toBytes("83360013f52598346f84ae26"));
                table.put(put);

        //    created_at, email, title, tracking_id,  user_id

        } catch (IOException e) {
            System.err.println("Exception while running HelloWorld: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }

    }

}