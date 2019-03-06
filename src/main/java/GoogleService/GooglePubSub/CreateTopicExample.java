package GoogleService.GooglePubSub;

import com.google.api.gax.rpc.ApiException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.ProjectTopicName;

public class CreateTopicExample {

    /**
     * Create a topic.
     *
     * @param args topicId
     * @throws Exception exception thrown if operation is unsuccessful
     */
    public static void main(String[] args) throws Exception {

        // Your Google Cloud Platform project ID
        String projectId = ServiceOptions.getDefaultProjectId();

        // Your topic ID, eg. "my-topic"
        String topicId = "CodeTopic";

        // Create a new topic
        ProjectTopicName topic = ProjectTopicName.of(projectId, topicId);
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            topicAdminClient.createTopic(topic);
        } catch (ApiException e) {
            // example : code = ALREADY_EXISTS(409) implies topic already exists
            System.out.print(e.getStatusCode().getCode());
            System.out.print(e.isRetryable());
        }

        System.out.printf("Topic %s:%s created.\n", topic.getProject(), topic.getTopic());
    }
}