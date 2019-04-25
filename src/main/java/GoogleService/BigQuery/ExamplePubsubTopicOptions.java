package GoogleService.BigQuery;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Options that can be used to configure Pub/Sub topic in Beam examples. */
public interface ExamplePubsubTopicOptions extends GcpOptions {
    @Description("Pub/Sub topic")
    @Default.InstanceFactory(PubsubTopicFactory.class)
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