package com.xxx;

import org.apache.pulsar.client.api.BatchReceivePolicy;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Messages;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.SubscriptionMode;
import org.apache.pulsar.client.api.SubscriptionType;
import org.apache.pulsar.shade.org.apache.commons.lang3.concurrent.BasicThreadFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * Hello world!
 */
public class App {

    private final static String SERVICE_URL = "pulsar://localhost:6650";
    private final static String TOPIC       = "persistent://study/app1/topic-1";

    private void createClient() throws PulsarClientException {
        try (PulsarClient client = PulsarClient.builder().serviceUrl(SERVICE_URL).build()) {
            System.out.println(client);
        }
    }

    private void testProduce() {
        try (PulsarClient client = PulsarClient.builder().serviceUrl(SERVICE_URL).build()) {
            Producer<byte[]> producer = client.newProducer().topic(TOPIC).create();
            MessageId messageId = producer.newMessage()
                                          .key("msgKey1")
                                          .value("hello".getBytes(StandardCharsets.UTF_8))
                                          .property("p1", "v1")
                                          .property("p2", "v2")
                                          .send();
            System.out.println("message id = " + messageId);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testConsume() {
        try {
            PulsarClient client = PulsarClient.builder().serviceUrl(SERVICE_URL).build();
            Consumer<byte[]> consumer = client.newConsumer()
                                              .topic(TOPIC)
                                              .subscriptionName("test-consumer")
                                              .subscriptionType(SubscriptionType.Exclusive)
                                              .subscriptionMode(SubscriptionMode.Durable)
                                              .subscribe();
            Message<byte[]> message = consumer.receive();
            System.out.println("message key = " + message.getKey());
            System.out.println("message data = " + new String(message.getData()));
            System.out.println("message properties = " + message.getProperties());
            consumer.acknowledge(message);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private void testBatchConsume() {
        try {
            PulsarClient client = PulsarClient.builder().serviceUrl(SERVICE_URL).build();
            // batch consume
            // 1. data size greater than 1024 * 1024
            // 2. message count greater than 100
            // 3. last consume interval greater than 5 seconds
            // any of the above conditions are met, message consumption is triggered
            Consumer<byte[]> consumer =
                    client.newConsumer()
                          .topic(TOPIC)
                          .subscriptionType(SubscriptionType.Exclusive)
                          .subscriptionName("test-batch-consumer")
                          .subscriptionMode(SubscriptionMode.Durable)
                          .batchReceivePolicy(BatchReceivePolicy.builder()
                                                                .maxNumBytes(1024 * 1024)
                                                                .maxNumMessages(100)
                                                                .timeout(5,
                                                                         TimeUnit.SECONDS)
                                                                .build())
                          .subscribe();

            Messages<byte[]> messages = consumer.batchReceive();
            for (Message<byte[]> message : messages) {
                System.out.println("batch message key = " + message.getKey());
                System.out.println("batch message data = " + new String(message.getData()));
                System.out.println("batch message properties = " + message.getProperties());
            }
            consumer.acknowledge(messages);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) throws PulsarClientException {
        App app = new App();
        app.createClient();
        BasicThreadFactory factory = new BasicThreadFactory.Builder().namingPattern("example-schedule-pool-%d")
                                                                     .daemon(false).build();
        ScheduledExecutorService executorService = new ScheduledThreadPoolExecutor(3, factory);
        executorService.execute(app::testProduce);
        executorService.execute(app::testConsume);
        executorService.execute(app::testBatchConsume);
    }
}
