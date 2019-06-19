package GoogleService.BigTable;

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
import org.joda.time.Duration;
import org.joda.time.Instant;


/**
 * 此函数用于将pub/sub topic数据写入bigtable
 *
 *
 */
public class PubSubToBigTable {
    private static final byte[] FAMILY = Bytes.toBytes("cf");
    private static final byte[] QUALIFIER = Bytes.toBytes("qualifier");
    static final int WINDOW_SIZE = 1; // Default window duration in minutes
    private static final int INJECTORNUMWORKERS = 1; //number of workers used for injecting

    //pubsub messages

    static final DoFn<String, Mutation> MUTATION_TRANSFORM = new DoFn<String, Mutation>() {
        private static final long serialVersionUID = 1L;
        //  { name: "John", age: 30, city: "New York" }
        @ProcessElement
        public void processElement(DoFn<String, Mutation>.ProcessContext c) throws Exception {

            JSONObject jsonObject = JSONObject.parseObject(c.element());

            Put put = new Put(Bytes.toBytes(jsonObject.get("name").toString()));

            put.addColumn(FAMILY, Bytes.toBytes("age"), Bytes.toBytes(jsonObject.get("age").toString()));
            put.addColumn(FAMILY, Bytes.toBytes("city"), Bytes.toBytes(jsonObject.get("city").toString()));

            c.output(put);
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

        p
                .apply(PubsubIO.readStrings().fromTopic(options.getPubsubTopic()))
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