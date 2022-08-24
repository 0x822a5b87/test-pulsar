package com.xxx.schema;

import com.xxx.App;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

/**
 * @author 0x822a5b87
 */
public class OrderConsumer {
    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = PulsarClient.builder().serviceUrl(App.SERVICE_URL).build();
        Consumer<Order> consumer = client.newConsumer(Schema.JSON(Order.class))
                                         .topic(OrderProducer.ORDER_TOPIC)
                                         .subscriptionName(OrderProducer.ORDER_TOPIC + "_" + OrderConsumer.class.getSimpleName())
                                         .subscribe();
        while (true) {
            Message<Order> message = consumer.receive();
            System.out.println("consume message : " + message.getValue());
            consumer.acknowledge(message);
        }
    }
}
