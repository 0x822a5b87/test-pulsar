package com.xxx.schema;

import com.xxx.App;
import org.apache.pulsar.client.api.MessageId;
import org.apache.pulsar.client.api.Producer;
import org.apache.pulsar.client.api.PulsarClient;
import org.apache.pulsar.client.api.PulsarClientException;
import org.apache.pulsar.client.api.Schema;

import java.util.concurrent.TimeUnit;

/**
 * @author 0x822a5b87
 */
public class OrderProducer {

    public static final String ORDER_TOPIC = "persistent://study/app1/topic-with-schema-order";

    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = PulsarClient.builder().serviceUrl(App.SERVICE_URL).build();
        Producer<Order> producer = client.newProducer(Schema.JSON(Order.class))
                                         .topic(ORDER_TOPIC)
                                         .create();
        Order order = new Order();
        order.setOrderId("1");
        order.setOrderName("1");
        MessageId id = producer.send(order);
        System.out.println("produce order message : " + id);

        id = producer.newMessage()
                     .deliverAfter(10, TimeUnit.SECONDS)
                     .value(order)
                     .send();
        System.out.println("produce delay order message : " + id);
    }
}
