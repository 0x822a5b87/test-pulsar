package main

import (
	"context"
	"fmt"
	"github.com/apache/pulsar-client-go/pulsar"
	"time"
)

var url = "pulsar://127.0.0.1:6650"
var topic = "persistent://study/app1/topic-1"

func newPulsarClient() (pulsar.Client, error) {
	return pulsar.NewClient(pulsar.ClientOptions{
		URL:               url,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
}

func startConsumer() {
	client, err := newPulsarClient()
	if err != nil {
		panic("error create consumer client")
	}
	defer client.Close()

	consumer, err := client.Subscribe(pulsar.ConsumerOptions{
		Topic:            topic,
		SubscriptionName: "sub-2",          // 订阅名称
		Type:             pulsar.Exclusive, // 订阅类型: 独占模式
		DLQ:              &pulsar.DLQPolicy{},
	})
	if err != nil {
		panic("error subscribe pulsar")
	}
	defer consumer.Close()

	for {
		msg, _ := consumer.Receive(context.Background())
		if err := processMsg(msg); err != nil {
			consumer.Nack(msg)
		} else {
			consumer.Ack(msg)
		}

	}
}

func processMsg(msg pulsar.Message) error {
	fmt.Printf("consume: %s \n", msg.Payload())
	return nil
}

func startProducer() {
	client, err := pulsar.NewClient(pulsar.ClientOptions{
		URL:               url,
		OperationTimeout:  30 * time.Second,
		ConnectionTimeout: 30 * time.Second,
	})
	if err != nil {
		panic("error create producer")
	}
	defer client.Close()

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Name:  "TestProducer-Go",
		Topic: topic,
	})
	defer producer.Close()
	msg := &pulsar.ProducerMessage{
		Key:     "msgKey1",
		Payload: []byte("hello go"),
		Properties: map[string]string{
			"p1": "v1",
			"p2": "v2",
		},
	}
	msgID, err := producer.Send(context.Background(), msg)
	fmt.Println(msgID)
}

func main() {
	startProducer()
	startConsumer()
}
