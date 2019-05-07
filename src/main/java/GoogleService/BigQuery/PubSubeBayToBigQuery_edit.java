package GoogleService.BigQuery;

import com.alibaba.fastjson.JSONObject;
import com.google.api.services.bigquery.model.TableFieldSchema;
import com.google.api.services.bigquery.model.TableRow;
import com.google.api.services.bigquery.model.TableSchema;
import com.google.common.collect.ImmutableList;
import org.apache.beam.sdk.Pipeline;
import org.apache.beam.sdk.PipelineResult;
import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.io.gcp.bigquery.BigQueryIO;
import org.apache.beam.sdk.io.gcp.pubsub.PubsubIO;
import org.apache.beam.sdk.options.*;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.ParDo;

import java.io.IOException;

/**
 * A streaming Beam Example using BigQuery output.
 *
 * <p>This pipeline example reads lines of the input text file, splits each line into individual
 * words, capitalizes those words, and writes the output to a BigQuery table.
 *
 * <p>The example is configured to use the default BigQuery table from the example common package
 * (there are no defaults for a general Beam pipeline). You can override them by using the {@literal
 * --bigQueryDataset}, and {@literal --bigQueryTable} options. If the BigQuery table do not exist,
 * the example will try to create them.
 *
 * <p>The example will try to cancel the pipelines on the signal to terminate the process (CTRL-C)
 * and then exits.
 */
public class PubSubeBayToBigQuery_edit {

    static final int WINDOW_SIZE = 1;

    /** A {@link DoFn} that tokenizes lines of text into individual words. */
    static class ExtractWords extends DoFn<String, String> {
        @ProcessElement
        public void processElement(ProcessContext c) {
            if (!c.element().isEmpty()) {
                c.output(c.element());
            }
        }
    }



    /** Converts strings into BigQuery rows. */
    static class StringToRowConverter extends DoFn<String, TableRow> {
        /** In this example, put the whole string into single BigQuery field. */
        @ProcessElement
        public void processElement(ProcessContext c) {
            JSONObject jsonObject = JSONObject.parseObject(c.element());


            c.output(new TableRow()
                    .set("string_field", jsonObject.get("string_field").toString())
                    .set("counts", jsonObject.get("counts"))
            );
        }

        static TableSchema getSchema() {
            return new TableSchema()
                    .setFields(
                            ImmutableList.of(
                                    new TableFieldSchema()
                                            .setName("string_field")
                                            .setType("STRING"),
                                    new TableFieldSchema()
                                            .setName("counts")
                                            .setType("INTEGER")
                            )
                    );
        }
    }



    public interface ExamplePubsubTopicOptions extends GcpOptions,ExampleOptions, ExampleBigQueryTableOptions, StreamingOptions{
        @Description("Pub/Sub topic")
        @Default.InstanceFactory(GoogleService.BigQuery.ExamplePubsubTopicOptions.PubsubTopicFactory.class)
        String getPubsubTopic();

        void setPubsubTopic(String topic);


        /** Returns a default Pub/Sub topic based on the project and the job names. */
        class PubsubTopicFactory implements DefaultValueFactory<String> {
            @Override
            public String create(PipelineOptions options) {
                return "projects/"
                        + options.as(GcpOptions.class).getProject()
                        + "/topics/"
                        + options.getJobName();
            }
        }
    }



    /**
     * Sets up and starts streaming pipeline.
     *
     * @throws IOException if there is a problem setting up resources
     *
     * --runner=DataflowRunner --project=aftership-team-data --stagingLocation=gs://data-test-us/bigtable --bigtableProjectId=aftership-team-data --bigtableInstanceId=test-demo --bigtableTableId=Dataflow_test --inputFile=gs://data-test-us/inputdata --pubsubTopic=projects/aftership-team-data/topics/shakes
     */
    public static void main(String[] args) throws IOException {
        ExamplePubsubTopicOptions options =
                PipelineOptionsFactory.fromArgs(args)
                        .withValidation()
                        .as(ExamplePubsubTopicOptions.class);
        options.setStreaming(true);

        options.setBigQuerySchema(StringToRowConverter.getSchema());
        ExampleUtils exampleUtils = new ExampleUtils(options);
        exampleUtils.setup();

        Pipeline pipeline = Pipeline.create(options);
        String tableSpec =
                new StringBuilder()
                        .append(options.getProject())
                        .append(":")
                        .append(options.getBigQueryDataset())
                        .append(".")
                        .append(options.getBigQueryTable())
                        .toString();

        pipeline
                .apply(PubsubIO.readStrings().fromTopic(options.getPubsubTopic()))      //get message form pubsub
                .apply(ParDo.of(new ExtractWords()))        // split messages
                .apply(ParDo.of(new StringToRowConverter()))
                .apply(
                        BigQueryIO.writeTableRows().to(tableSpec).withSchema(StringToRowConverter.getSchema())      // table schema
                                .withWriteDisposition(BigQueryIO.Write.WriteDisposition.WRITE_APPEND));         // append to table



        PipelineResult result = pipeline.run();

        // ExampleUtils will try to cancel the pipeline before the program exists.
        exampleUtils.waitToFinish(result);
    }
}

