package GoogleService.GooglePubSub;

import MongoDB.MongoDBUtil;
import com.google.api.core.ApiFuture;
import com.google.api.core.ApiFutureCallback;
import com.google.api.core.ApiFutures;
import com.google.api.gax.rpc.ApiException;
import com.google.cloud.ServiceOptions;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.protobuf.ByteString;
import com.google.pubsub.v1.ProjectTopicName;
import com.google.pubsub.v1.PubsubMessage;
import org.apache.log4j.Logger;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class PublishMessage {

    static Logger log = Logger.getLogger(PublishMessage.class.getClass());

    public static void publishMessages(List<String> messages, String projectId, String topic) throws Exception {
        // [START pubsub_publish]
        // Your Google Cloud Platform project ID
       // String projectId = ServiceOptions.getDefaultProjectId();

        ProjectTopicName topicName = ProjectTopicName.of(projectId, topic);
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        try {

            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

           // List<String> messages = Arrays.asList("first message", "second message");

            // schedule publishing one message at a time : messages get automatically batched
            for (String message : messages) {
                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> messageIdFuture = publisher.publish(pubsubMessage);
                messageIdFutures.add(messageIdFuture);
            }
        } finally {
            // wait on any pending publish requests.
            List<String> messageIds = ApiFutures.allAsList(messageIdFutures).get();

/*            for (String messageId : messageIds) {
                System.out.println("published with message ID: " + messageId);
            }*/

            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
               // publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
        // [END pubsub_publish]
    }

    public static void publishMessagesWithErrorHandler(List<String> messages, String projectId, String topic) throws Exception {
        // [START pubsub_publish_error_handler]
        ProjectTopicName topicName = ProjectTopicName.of(projectId, topic);
        Publisher publisher = null;
        List<ApiFuture<String>> messageIdFutures = new ArrayList<>();

        try {
            // Create a publisher instance with default settings bound to the topic
            publisher = Publisher.newBuilder(topicName).build();

           // List<String> messages = Arrays.asList("first message", "second message");

            for (final String message : messages) {

                System.out.println("topic message is:" + message);

                ByteString data = ByteString.copyFromUtf8(message);
                PubsubMessage pubsubMessage = PubsubMessage.newBuilder().setData(data).build();

                // Once published, returns a server-assigned message id (unique within the topic)
                ApiFuture<String> future = publisher.publish(pubsubMessage);

                // Add an asynchronous callback to handle success / failure
                ApiFutures.addCallback(
                        future,
                        new ApiFutureCallback<String>() {

                            @Override
                            public void onFailure(Throwable throwable) {
                                if (throwable instanceof ApiException) {
                                    ApiException apiException = ((ApiException) throwable);
                                    // details on the API exception
                                    log.error(apiException.getStatusCode().getCode());
                                    log.error(apiException.isRetryable());
                                }
                                log.error("Error publishing message : " + message);
                            }

                            @Override
                            public void onSuccess(String messageId) {
                                // Once published, returns server-assigned message ids (unique within the topic)
                                log.info("message ids:" + messageId);
                            }
                        },
                        MoreExecutors.directExecutor());
            }
        } finally {
            if (publisher != null) {
                // When finished with the publisher, shutdown to free up resources.
                publisher.shutdown();
                publisher.awaitTermination(1, TimeUnit.MINUTES);
            }
        }
        // [END pubsub_publish_error_handler]
    }


}
