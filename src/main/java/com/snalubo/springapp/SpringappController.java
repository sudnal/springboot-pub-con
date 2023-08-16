package com.snalubo.springapp;

import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.google.cloud.pubsub.v1.AckReplyConsumer;
import com.google.cloud.pubsub.v1.MessageReceiver;
import com.google.cloud.pubsub.v1.Subscriber;
import com.google.pubsub.v1.ProjectSubscriptionName;
import com.google.pubsub.v1.PubsubMessage;

import com.google.api.core.ApiFuture;
import com.google.cloud.pubsub.v1.Publisher;
import com.google.cloud.pubsub.v1.TopicAdminClient;
import com.google.pubsub.v1.Encoding;
import com.google.pubsub.v1.PubsubMessage;
import com.google.pubsub.v1.TopicName;
import com.google.protobuf.ByteString;
import com.google.protobuf.util.JsonFormat;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.ExecutionException;
import java.io.IOException;


import com.snalubo.utilities.PersonProto.Person;

@RestController
@RequestMapping("/pubsub")
public class SpringappController {

    @RequestMapping("/publish")
    public String publishMessages() throws Exception {
        String projectId = "kinetic-dryad-395306";
        String topicId = "gcp-pubsub-demo";
        publishEg(projectId, topicId);

        return "Messages published:: ";

    }

    private static void publishEg(String projectId, String topicId)
            throws IOException, ExecutionException, InterruptedException {

        Encoding encoding = null;

        TopicName topicName = TopicName.of(projectId, topicId);

        // Get the topic encoding type.
        try (TopicAdminClient topicAdminClient = TopicAdminClient.create()) {
            encoding = topicAdminClient.getTopic(topicName).getSchemaSettings().getEncoding();
        }

        Publisher publisher = null;

        // Instantiate a protoc-generated class defined in `person.proto`.
        Person person = Person.newBuilder().setName("Suheer").setHeight(5.5f).setIsMale(true).build();

        block:
        try {
            publisher = Publisher.newBuilder(topicName).build();

            PubsubMessage.Builder message = PubsubMessage.newBuilder();

            // Prepare an appropriately formatted message based on topic encoding.
            switch (encoding) {
                case BINARY:
                    message.setData(person.toByteString());
                    System.out.println("Publishing a BINARY-formatted message:\n" + message);
                    break;

                case JSON:
                    String jsonString = JsonFormat.printer().omittingInsignificantWhitespace().print(person);
                    message.setData(ByteString.copyFromUtf8(jsonString));
                    System.out.println("Publishing a JSON-formatted message:\n" + message);
                    break;

                default:
                    break block;
            }

            // Publish the message.
            ApiFuture<String> future = publisher.publish(message.build());
            System.out.println("Published message ID: " + future.get());

        } finally {
            if (publisher != null) {
                publisher.shutdown();
                publisher.awaitTermination(30, TimeUnit.SECONDS);
            }
        }
    }


    @RequestMapping("/consume")
    public String consumeMessages() {

        String projectId = "kinetic-dryad-395306";
        String subscriptionId = "gcp-pubsub-demo-sub1";
        subscribeEg(projectId, subscriptionId);

        return "Messages consumed:: ";
    }

    private static void subscribeEg(String projectId, String subscriptionId) {
        ProjectSubscriptionName subscriptionName = ProjectSubscriptionName.of(projectId, subscriptionId);

        MessageReceiver receiver = (PubsubMessage message, AckReplyConsumer consumer) -> {
            System.out.println("Id: " + message.getMessageId());
            System.out.println("Data: " + message.getData().toStringUtf8());
            consumer.ack();
        };

        Subscriber subscriber = null;
        try {
            subscriber = Subscriber.newBuilder(subscriptionName, receiver).build();
            subscriber.startAsync().awaitRunning();
            System.out.printf("Listening for messages on %s: \n", subscriptionName);
            subscriber.awaitTerminated(10, TimeUnit.SECONDS);
        } catch(TimeoutException e) {
            subscriber.stopAsync();
        }
    }
}
