package Ebay;

import GoogleService.BigTable.CloudBigtableOptions;
import com.alibaba.fastjson.JSONObject;
import com.google.cloud.bigtable.beam.CloudBigtableIO;
import com.google.cloud.bigtable.beam.CloudBigtableTableConfiguration;
import org.apache.beam.runners.dataflow.DataflowRunner;
import org.apache.beam.runners.dataflow.options.DataflowPipelineOptions;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.io.TextIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.PipelineOptionsFactory;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.windowing.FixedWindows;
import org.apache.beam.sdk.transforms.windowing.Window;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.log4j.Logger;
import org.joda.time.Duration;
import org.joda.time.Instant;



public class PubSubeBayToBigTable {
    private static final byte[] FAMILY = Bytes.toBytes("cf");
    private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
    static final int WINDOW_SIZE = 1; // Default window duration in minutes
    private static final int INJECTORNUMWORKERS = 1; //number of workers used for injecting
    static Logger log = Logger.getLogger(Mongodb.class.getClass());

    /**
     * 此函数用于将数据写入bigtable
     */

    static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
        private static final long serialVersionUID = 1L;
        @ProcessElement
        public void processElement(ProcessContext c) throws Exception {

            JSONObject jsonObject = JSONObject.parseObject(c.element());

            try {
                Put put = new Put(Bytes.toBytes(jsonObject.get("rowKey").toString()));

                put.addColumn(FAMILY, Bytes.toBytes("date"), Bytes.toBytes(jsonObject.get("date").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("utc_offset"), Bytes.toBytes(jsonObject.get("utc_offset").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("destination_country"), Bytes.toBytes(jsonObject.get("destination_country").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("checkpoint_created_at"), Bytes.toBytes(jsonObject.get("checkpoint_created_at").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("user_provided_carrier_name"), Bytes.toBytes(jsonObject.get("user_provided_carrier_name").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("substatus"), Bytes.toBytes(jsonObject.get("substatus").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("first_checkpoint_time"), Bytes.toBytes(jsonObject.get("first_checkpoint_time").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("message"), Bytes.toBytes(jsonObject.get("message").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("origin_country"), Bytes.toBytes(jsonObject.get("origin_country").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("user_id"), Bytes.toBytes(jsonObject.get("user_id").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("tracking_number"), Bytes.toBytes(jsonObject.get("tracking_number").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("time"), Bytes.toBytes(jsonObject.get("time").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("slug"), Bytes.toBytes(jsonObject.get("slug").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("tracking_id"), Bytes.toBytes(jsonObject.get("tracking_id").toString()));
                put.addColumn(FAMILY, Bytes.toBytes("status"), Bytes.toBytes(jsonObject.get("status").toString()));
                c.output(put);
            }
            catch (Exception e){
                log.error( e.getClass().getName() + ": " + e.getMessage() );
                log.error("Insert to big table xception message: " + jsonObject.toString());
            }

        }
    };

    /**
     * Extracts words from a line and append the line's timestamp to each word, so that we can use a
     * Put instead of Increment for each word when we write them to CBT. This information will have to
     * be processed later to get a complete word count across time. The idea here is that Puts are
     * idempotent, so if a Dataflow job fails midway and is restarted, you still get accurate results,
     * even if the Put was sent two times. To get a complete word count, you'd have to perform a
     * prefix scan for the word + "|" and sum the count across the various rows.
     */
    static class ExtractWordsFn extends DoFn<String, String> {
        private static final long serialVersionUID = 0;

        @ProcessElement
        public void processElement(ProcessContext c) {
            Instant timestamp = c.timestamp();

            if (!c.element().isEmpty()) {
                c.output(c.element());
            }

        }
    }



    public static interface BigtablePubsubOptions extends CloudBigtableOptions {
        @Default.Integer(WINDOW_SIZE)
        Integer getWindowSize();

        void setWindowSize(Integer value);

        String getPubsubTopic();

        void setPubsubTopic(String pubsubTopic);

        String getInputFile();

        void setInputFile(String location);
    }

    /**
     * <p>Creates a dataflow pipeline that creates the following chain:</p>
     * <ol>
     *   <li> Reads from a Cloud Pubsub topic
     *   <li> Window into fixed windows of 1 minute
     *   <li> Applies word count transform
     *   <li> Creates Puts from each of the word counts in the array
     *   <li> Performs a Bigtable Put on the items
     * </ol>
     *
     * @param args Arguments to use to configure the Dataflow Pipeline.  The first three are required
     *   when running via managed resource in Google Cloud Platform.  Those options should be omitted
     *   for LOCAL runs.  The next four arguments are to configure the Bigtable connection. The last
     *   two items are for Cloud Pubsub.
     *        --runner=BlockingDataflowPipelineRunner
     *        --project=[dataflow project] \\
     *        --stagingLocation=gs://[your google storage bucket] \\
     *        --bigtableProjectId=[bigtable project] \\
     *        --bigtableInstanceId=[bigtable instance id] \\
     *        --bigtableTableId=[bigtable tableName]
     *        --inputFile=[file path on GCS]
     *        --pubsubTopic=projects/[project name]/topics/[topic name]
     */

    public static void main(String[] args) throws Exception {
        // CloudBigtableOptions is one way to retrieve the options.  It's not required.
        BigtablePubsubOptions options =
                PipelineOptionsFactory.fromArgs(args).withValidation().as(BigtablePubsubOptions.class);

        // CloudBigtableTableConfiguration contains the project, instance and table to connect to.
        CloudBigtableTableConfiguration config =
                new CloudBigtableTableConfiguration.Builder()
                        .withProjectId(options.getBigtableProjectId())
                        .withInstanceId(options.getBigtableInstanceId())
                        .withTableId(options.getBigtableTableId())
                        .build();

        // In order to cancel the pipelines automatically,
        // DataflowPipelineRunner is forced to be used.
        // Also enables the 2 jobs to run at the same time.
        options.setRunner(DataflowRunner.class);

        options.as(DataflowPipelineOptions.class).setStreaming(true);
        Pipeline p = Pipeline.create(options);

        FixedWindows window = FixedWindows.of(Duration.standardMinutes(options.getWindowSize()));

        p.apply(PubsubIO.readStrings().fromTopic(options.getPubsubTopic()))
                .apply(Window.<String> into(window))
                .apply(ParDo.of(new ExtractWordsFn()))
                .apply(ParDo.of(MUTATION_TRANSFORM))
                .apply(CloudBigtableIO.writeToTable(config));

        p.run().waitUntilFinish();
        // Start a second job to inject messages into a Cloud Pubsub topic
        injectMessages(options);
    }


    private static void injectMessages(BigtablePubsubOptions options) {
        String inputFile = options.getInputFile();
        String topic = options.getPubsubTopic();
        DataflowPipelineOptions copiedOptions = options.as(DataflowPipelineOptions.class);
        copiedOptions.setStreaming(false);
        copiedOptions.setNumWorkers(INJECTORNUMWORKERS);
        copiedOptions.setJobName(copiedOptions.getJobName() + "-injector");
        Pipeline injectorPipeline = Pipeline.create(copiedOptions);
        injectorPipeline.apply(TextIO.read().from(inputFile))
                .apply(ParDo.of(new FilterEmptyStringsFn()))
                .apply(PubsubIO.writeStrings().to(topic));
        injectorPipeline.run().waitUntilFinish();
    }

    static class FilterEmptyStringsFn extends DoFn<String, String> {
        private static final long serialVersionUID = 0;

        @ProcessElement
        public void processElement(ProcessContext c) {
            if (!"".equals(c.element())) {
                c.output(c.element());
            }
        }
    }
}