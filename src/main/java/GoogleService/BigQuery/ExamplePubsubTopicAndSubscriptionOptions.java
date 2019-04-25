package GoogleService.BigQuery;

import org.apache.beam.sdk.extensions.gcp.options.GcpOptions;
import org.apache.beam.sdk.options.Default;
import org.apache.beam.sdk.options.DefaultValueFactory;
import org.apache.beam.sdk.options.Description;
import org.apache.beam.sdk.options.PipelineOptions;

/** Options that can be used to configure Pub/Sub topic/subscription in Beam examples. */
public interface ExamplePubsubTopicAndSubscriptionOptions extends ExamplePubsubTopicOptions {
    @Description("Pub/Sub subscription")
    @Default.InstanceFactory(PubsubSubscriptionFactory.class)
    String getPubsubSubscription();

    void setPubsubSubscription(String subscription);

    /** Returns a default Pub/Sub subscription based on the project and the job names. */
    class PubsubSubscriptionFactory implements DefaultValueFactory<String> {
        @Override
        public String create(PipelineOptions options) {
            return "projects/"
                    + options.as(GcpOptions.class).getProject()
                    + "/subscriptions/"
                    + options.getJobName();
        }
    }
}