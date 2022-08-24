# test-pulsar
> test pulsar

## reference

- example

  - [Apache Pulsar学习笔记01: Pulsar简介](https://blog.frognew.com/2021/10/learning-apache-pulsar-01.html)

  - [Apache Pulsar学习笔记02: 初步了解Pulsar的架构，单个Pulsar集群的组成](https://blog.frognew.com/2021/10/learning-apache-pulsar-02.html)

  - [Apache Pulsar学习笔记03: 本地开发环境docker容器中运行Pulsar](https://blog.frognew.com/2021/10/learning-apache-pulsar-03.html)

  - [Apache Pulsar学习笔记04: Pulsar的租户、命名空间和Topic](https://blog.frognew.com/2021/10/learning-apache-pulsar-04.html)

  - [Apache Pulsar学习笔记05: Pulsar的生产者、消费者、订阅和订阅模式](https://blog.frognew.com/2021/10/learning-apache-pulsar-05.html)

  - [Apache Pulsar学习笔记06: Pulsar的Java客户端库](https://blog.frognew.com/2021/10/learning-apache-pulsar-06.html)

  - [Apache Pulsar学习笔记07: Pulsar的Go客户端库](https://blog.frognew.com/2021/10/learning-apache-pulsar-07.html)

  - [Apache Pulsar学习笔记08: 使用Pulsar Schema管理消息数据的类型安全性](https://blog.frognew.com/2021/10/learning-apache-pulsar-08.html)

  - [Apache Pulsar学习笔记09: Pulsar的Web图形化管理工具Pulsar Manager](https://blog.frognew.com/2021/10/learning-apache-pulsar-09.html)

  - [Apache Pulsar学习笔记10: 延迟消息投递](https://blog.frognew.com/2021/11/learning-apache-pulsar-10.html)

  - [Apache Pulsar学习笔记11: 使用分区Topic](https://blog.frognew.com/2021/11/learning-apache-pulsar-11.html)

  - [Apache Pulsar学习笔记12: 开启基于JWT的身份认证](https://blog.frognew.com/2021/11/learning-apache-pulsar-12.html)

  - [Apache Pulsar学习笔记13: 授权和权限管理](https://blog.frognew.com/2021/11/learning-apache-pulsar-13.html)

## example

### Apache Pulsar学习笔记01: Pulsar简介

> 在分布式消息系统集群中分布消息数据时，有两种不同的方法: `基于分区(Partion-based)`和`基于段(Segment-based)`。
>
> 在基于分区的消息存储结构中，Topic被划分为固定数量的分区，发布到Topic中的数据均匀的分布在分区中，同时为了确保数据冗余，每个分区会被复制到不同的节点。如下例图所示，每个分区接收到的发布到Topic的消息总数的三分之一。Topic中消息的总存储量为Topic的分区数乘以每个分区的大小，如果达到这个限制，需要向集群添加更多的节点同时增加Topic的分区数量才能继续向分区中添加数据，增加分区的数量还需要执行重新平衡，这是一个十分复杂和耗时的过程。在基于分区的消息存储结构的分布式消息系统中，一般在创建Topic的时候需要预先确定分区数量，但这样做有一些缺点，因为单个分区只会存储在集群中单个集群节点上，因此单个分区的大小就受限于该节点上的硬盘空间大小，由于Topic中的数据均匀分布在所有分区中，所以如果集群节点的硬盘容量不一样的话，那么Topic的每个分区的大小将限制为最小硬盘容量的节点。当Topic达到容量限制后，唯一能做的就是增加Topic的分区数量，但这个扩容的过程包括重新平衡整个Topic，Topic中的数据将被重新分布到该Topic的所有分区中，平衡数据的过程十分消耗网络带宽和磁盘I/O。 Kafka采用的是基于分区的消息存储架构。

![kafka-partitions](img/example/kafka-partitions.png)

> **Pulsar采用的是基于段的消息存储结构。**
>
> Pulsar依赖Apache BookKeeper项目来实现消息的持久存储，BookKeeper的逻辑存储模型是基于无限流记录作为顺序日志存储的概念。从下图可以看出，在BookKeeper中，每个日志被分解成更小的数据块，称为`段(Segament)`，这些数据块又由多个日志条目组成。然后，为了实现冗余和扩展，这些段会在存储层中被称为`bookies`的多个节点上写入。可以将段放在集群节点具有足够硬盘容量的任何位置，当没有足够的空间用于存储新的段时，可以方便地添加节点并立即存储数据。基于段的存储架构的优点在于可以实现真正的水平伸缩，段可以被无限创建并存储在任何位置。

![pulsar-segaments](img/example/pulsar-segaments.png)

### Apache Pulsar学习笔记02: 初步了解Pulsar的架构，单个Pulsar集群的组成

> Pulsar提供了一个比`Pulsar Cluster(Pulsar集群)`更高级别的抽象，叫`Pulsar Instance(Pulsar实例)`。一个Pulsar Instance由多个Pulsar Cluster组成，一个Pulsar实例中的集群之间可以相互跨地域复制数据。
>
> 单个的Pulsar集群由以下几部分组成:
>
> - Pulsar Proxy: 即Pulsar代理，一个集群可以有一个或多个Pulsar代理，Pulsar代理是`无状态`的，Proxy作为集群的`智能路由层`，是负责Pulsar客户端与Pulsar集群交互的统一网关。
> - Pulsar Brokers: 一个集群有多个Broker，Broker也是`无状态`的，Broker作为集群的`服务层`，Pulsar代理会将客户端的访问请求转发到正确的Broker上。Broker作为服务层与Pulsar的存储层进行交互，负责处理和负载均衡Producer客户端发出的消息，并将这些消息分派给Consumer客户端。
> - Bookies: 一个集群有多个Bookie节点(BookKeeper节点组成Bookeeper集群)负责消息的持久化存储。
> - Zookeeper集群: 用于集群级别的配置和协调，Zookeeper中存储Pulsar集群的所有元数据(例如Topic元数据、Schema、Broker负载数据等等)

![pulsar-cluster](img/example/pulsar-cluster.png)

### Apache Pulsar学习笔记03: 本地开发环境docker容器中运行Pulsar

#### 以docker容器运行单机Pulsar

> Pulsar 需要使用两个端口：
>
> - bootstrap service port，默认为 `8080`，Pulsar的Web接口在8080端口上。
> - broker port，默认为 `6650`，Pulsar的客户端使用这个端口来从Pulsar发送和接收数据。

```bash
docker pull apachepulsar/pulsar:2.8.1

docker volume create pulsar-data
docker volume create pulsar-conf

docker run -d -p 6650:6650  -p 8080:8080 --privileged \
-v pulsar-data:/pulsar/data -v pulsar-conf:/pulsar/conf \
--name pulsar \
apachepulsar/pulsar:2.8.1 bin/pulsar standalone
```

#### 命令行工具pulsar-admin

> 1. 使用`pulsar-admin`可以从一个点管理整个实例，包括所有集群。
> 2. pulsar在逻辑架构上由租户(Tenant)、命名空间(Namespace)、主题(Topic)组成。结构是：`persistent://tenant/namespace/topic`。

```bash
# 进入容器
docker exec -it pulsar /bin/bash
cd /pulsar/bin

# 查看集群
./pulsar-admin clusters list
# 查看租户
./pulsar-admin tenants list

# 创建租户 study
./pulsar-admin tenants create study
# 在租户 study 下创建 namespace app1
./pulsar-admin namespaces create study/app1
# 在 study/app1 下创建 topic topic-1
./pulsar-admin topics create persistent://study/app1/topic-1
```

### Apache Pulsar学习笔记04: Pulsar的租户、命名空间和Topic

> Pulsar被设计为一个多租户系统，租户可以跨集群分布，每个租户都可以有单独的认证和授权机制，可以针对租户设置存储配额、消息生存时间TTL和隔离策略。

![pulsar-logical-arch](img/example/pulsar-logical-arch.png)

#### 租户

> 租户可以跨集群分布，表了组织中特定的业务单元，产品线、核心功能，这些由组织不同的部门或团队负责。

#### 命名空间(Namespace)

>命名空间是租户的管理单元，每个租户下可以创建多个命名空间。可以通过在命名空间上设置的配置策略来管理该命名空间下的Topic，这样就可以在命名空间的级别上为该命名空间的所有Topic设置访问权限、调整复制设置、管理跨集群跨地域的消息复制，控制消息过期时间。

#### 主题(Topic)和分区主题(Partitioned Topic)

>在Pulsar中所有消息的读取和写入都是和Topic进行，Pulsar的Topic本身并不区分`发布订阅模式`或者`生产消费模式(独占, 一个消息只能被一个消费者消费)`，Pulsar是依赖于各种`订阅类型`来控制消息的使用模式。

> Pulsar 支持分区主题（partitioned topic），内部将分区分为多个内部topic这样就可以多台broker提供服务，并且在用户无感知的情况下扩容分区了。

#### Topic的URL，持久化Topic和非持久化Topic

> pulsar 支持持久化topic和非持久化topic，他们的url分别为：
>
> - persistent://tenant/namespace/topic-name
> - non-persistent://tenant/namespace/topic-name

### Apache Pulsar学习笔记05: Pulsar的生产者、消费者、订阅和订阅模式

> 四个核心概念：
>
> - producer：Producer是一个连接到Pulsar Broker(或Pulsar Proxy)上的进程，Producer发送消息到Topic。
> - consumer：Consumer也是一个连接到Pulsar Broker(或Pulsar Proxy)上的进程，是从Broker接收消息的进程，Consumer成功处理了一条消息后需要向Broker发送确认(ACK)，以让Broker知道消息已经被接收和处理，如果Broker在预先设置的时间范围内没有收到确认(ACK)， Broker可以将消息重新发送订阅该Topic的Consumer。
> - Subscription-name：类似于 kafka 的 consumer group，但是可以配合 `Subscription-type` 来实现一些比较复杂的功能
> - Subscription-type：订阅类型

#### 订阅和订阅模式

>Pulsar支持4种订阅模式:
>
>- exclusive(独占模式)
>- failover(故障转移模式，也叫灾备模式)
>- shared(共享模式)
>- key-shared(基于key的共享模式)

#####  exclusive(独占模式)

> 独占模式，只能有一个Consumer绑定到订阅上。如果多于一个Consumer尝试以使用相同的订阅订阅Topic，就会抛出异常且无法连接。

![pulsar-exclusive-subscriptions](img/example/pulsar-exclusive-subscriptions.png)

#####  failover(灾备模式)

> 可以启动多个 consumer，但是只有一个 consumer 会消费数据，其他的 consumer 都是某一个 consumer 的备份。

![pulsar-failover-subscriptions](img/example/pulsar-failover-subscriptions.png)

##### shared(共享模式)

> 多个消费者任务并行的消费pulsar，每条消息只会被一个消费者任务消费到。
>
> 1. 无法保证数据顺序消费；
> 2. 不可使用 cumulative ack

![pulsar-shared-subscriptions](img/example/pulsar-shared-subscriptions.png)

#####  key-shared(基于key的共享模式)

> `key-shared` 是 `shared` 模式的一个特例，对于相同的 `key` 的消息会被分配给一个消费者实例。

####  使用pulsar-client

```bash
# 创建 topic
pulsar-admin topics create persistent://study/app1/topic-1

# 查看topic
pulsar-admin topics list study/app1
```

> 使用 `pulsar-client` 启动一个消费者，消费者名为 `sub-1`，消费类型为 `Exclusive`

```bash
./pulsar-client consume \
persistent://study/app1/topic-1 \
--num-messages 0 \
--subscription-name sub-1 \
--subscription-type Exclusive
```

> 发送消息

```bash
./pulsar-client produce persistent://study/app1/topic-1 --num-produce 2 --messages "helloworld"
```

### Apache Pulsar学习笔记06: Pulsar的Java客户端库

```java
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
```

#### 3.6 死信策略

> 当消费者处理消息失败时，如果消费者给了Broker否定确认(negative ack)，或者Broker在预先设置的时间内没有收到确认ACK，Broker可以将消息重新发送给消息者。 这就相当于"重试"逻辑，如果重试成功，消息会被正常消费。但总会有重试无法成功的情况，因此不能无限的重试下去。
>
> 关于消费者处理消息时的异常处理有以下三种选择:
>
> - 第一种方法是捕获任何异常，无论是否发生异常都简单地确认这些消息已成功处理，这实际上是忽略了处理失败的消息。这种方法只适用于业务上允许消息丢失的场景。
> - 第二种方法是上面说的无限重试，消息处理成功发确认ACK，消息处理失败捕获异常时发送否定确认(negative ack)。这种方法可能会导致失败消息的无限重新传递循环，可能会引起消息堵塞，导致后边的消息无法被消费。
> - 第三种方法是将有问题的消息路由到一个单独的主题，称为死信主题（Dead Letter Topic)。这样就能避免无限的重新传递循环引起消息堵塞，同时死信主题中保留的消息可以在后续进行进一步有程序自动处理或者人工检查和处理。

```java
Consumer consumer = client.newConsumer()
    .topic("persistent://study/app1/topic-1")
    .subscriptionName("sub5")
    .deadLetterPolicy(DeadLetterPolicy.builder()
    .maxRedeliverCount(10)
    .deadLetterTopic("persistent://study/app1/dlt-1"”))
    .subscribe();
```

### Apache Pulsar学习笔记07: Pulsar的Go客户端库

```go
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
		DLQ: &pulsar.DLQPolicy{},
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
```

### Apache Pulsar学习笔记08: 使用Pulsar Schema管理消息数据的类型安全性

> 大部分主流的消息队列的类型安全基本都是基于生产者指定序列化，消费者指定反序列化来决定的。例如kafka的是通过：
>
> ```java
> props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
> props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "com.baeldung.kafka.serdes.CustomSerializer");
> ```
>
> 而大部分消息队列这个数据是不存在服务端的，也就是存在用户传错的可能性。
>
> **pulsar 支持了这种序列化方式，同时还提供了一种服务端的方法即Pulsar Schema**
>
> Pulsar有一个内置的Schema Registry，允许客户端为每个Topic上传消息数据的Schema。这样Producer和Consumer就可以通知Pulsar要通过Topic传输什么类型的数据。 使用Schema，Pulsar可以强制执行类型安全，确保生产者和消费者保持同步，客户端不需要再手动进行消息的序列化和反序列化，而由Pulsar Schema在后台执行这些操作。

#### schema

> 在golang中schema如下

```go
type SchemaInfo struct {
	Name       string
	Schema     string
	Type       SchemaType
	Properties map[string]string
}
```

> 在java中schema如下

```java
@InterfaceAudience.Public
@InterfaceStability.Stable
public interface SchemaInfo {

    String getName();

    /**
     * The schema data in AVRO JSON format.
     */
    byte[] getSchema();

    /**
     * The type of schema (AVRO, JSON, PROTOBUF, etc..).
     */
    SchemaType getType();

    /**
     * Additional properties of the schema definition (implementation defined).
     */
    Map<String, String> getProperties();

    String getSchemaDefinition();

    static SchemaInfoBuilder builder() {
        return new SchemaInfoBuilder();
    }
}
```



#### consumer

```java
public class OrderConsumer {
    public static void main(String[] args) throws PulsarClientException {
        PulsarClient client = PulsarClient.builder().serviceUrl(App.SERVICE_URL).build();
        Consumer<Order> consumer = client.newConsumer(Schema.JSON(Order.class))
                                         .topic(OrderProducer.ORDER_TOPIC)
                                         .subscriptionName(OrderProducer.ORDER_TOPIC + "_" + OrderConsumer.class.getSimpleName())
                                         .subscribe();
        Message<Order> message = consumer.receive();
        System.out.println("consume message : " + message.getValue());
        consumer.acknowledge(message);
    }
}
```

#### producer

```java
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
        producer.close();
        System.out.println("produce order message : " + id);
    }
}
```

#### order

```java
public class Order {
    private String orderId;
    private String orderName;

    public String getOrderId() {
        return orderId;
    }

    public void setOrderId(String orderId) {
        this.orderId = orderId;
    }

    public String getOrderName() {
        return orderName;
    }

    public void setOrderName(String orderName) {
        this.orderName = orderName;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this)
                .append("orderId", orderId)
                .append("orderName", orderName)
                .toString();
    }
}
```

#### 查看 schema

```bash
./bin/pulsar-admin schemas get persistent://study/app1/topic-with-schema-order
```

```json
{
  "version": 0,
  "schemaInfo": {
    "name": "topic-with-schema-order",
    "schema": {
      "type": "record",
      "name": "Order",
      "namespace": "com.xxx.schema",
      "fields": [
        {
          "name": "orderId",
          "type": [
            "null",
            "string"
          ]
        },
        {
          "name": "orderName",
          "type": [
            "null",
            "string"
          ]
        }
      ]
    },
    "type": "JSON",
    "properties": {
      "__alwaysAllowNull": "true",
      "__jsr310ConversionEnabled": "false"
    }
  }
}
```

### Apache Pulsar学习笔记09: Pulsar的Web图形化管理工具Pulsar Manager

> 启动 pulsar manager

```bash
docker pull apachepulsar/pulsar-manager:v0.2.0

docker volume create pulsar-manager-data

docker run -d \
    -v pulsar-manager-data:/pulsar-manager/pulsar-manager/dbdata \
    -p 9527:9527 -p 7750:7750 \
    -e SPRING_CONFIGURATION_FILE=/pulsar-manager/pulsar-manager/application.properties \
    --name pulsar-manager \
    apachepulsar/pulsar-manager:v0.2.0
```

> 设置账号密码为 `admin` && `apachepulsar`

```bash
CSRF_TOKEN=$(curl http://localhost:7750/pulsar-manager/csrf-token)
curl \
   -H 'X-XSRF-TOKEN: $CSRF_TOKEN' \
   -H 'Cookie: XSRF-TOKEN=$CSRF_TOKEN;' \
   -H "Content-Type: application/json" \
   -X PUT http://localhost:7750/pulsar-manager/users/superuser \
   -d '{"name": "admin", "password": "apachepulsar", "description": "test", "email": "username@test.org"}'
```

> 在 **http://127.0.0.1:9527** 访问 UI，注意，由于是在docker中启动，所以：
>
> 1. 如果 pulsar 也是在docker中启动，则pulsar要么暴露端口到物理机，要么和 pulsar manager 在一个UI地址中；
> 2. 如果 pulsar 不在docker中启动，在macos上需要通过 `docker.for.mac.localhost` 来访问；

### Apache Pulsar学习笔记11: 使用分区Topic

> 1. **在Pulsar中一个Topic只能由一个Broker提供服务**
> 2. Pulsar`通过分区Topic来提高吞吐量`，分区Topic在底层通过N个内部Topic实现，N就是分区的数量。
> 3. 每个分区(内部的Topic)还是由一个Broker提供服务

![pulsar-topic-partitioning](img/example/pulsar-topic-partitioning.png)

#### 路由模式

> 对于没有提供key的情况下，有三种路由策略：
>
> 1. `SinglePartition` 连接到某个特定的分区，并将该producer所有的数据写入该分区；
> 2. `RoundRobinPartition` 轮询将数据写入随机的分区；
> 3. `CustomPartition` 按照用户自定义选择分区；
>
> 对于提供key的情况下：
>
> 1.  `SinglePartition` 和 `RoundRobinPartition` 按照key的hash选择分区，也就是说具有相同key的消息会写入相同的分区；

#### 使用路由模式

> 使用 `SinglePartition`

```java
Producer<byte[]> producer = pulsarClient.newProducer()
        .topic(topic)
        .messageRoutingMode(MessageRoutingMode.SinglePartition)
        .create();
producer.send("Partitioned topic message".getBytes());
```

> 自定义 `MessageRouter`

```java
Producer<byte[]> producer = client.newProducer().topic("persistent://study/app1/topic-4").messageRouter(new MessageRouter() {
    @Override
    public int choosePartition(Message<?> msg, TopicMetadata metadata) {
        return 0; // 所有消息都被发送到分区0
    }
}).create();
```

### Apache Pulsar学习笔记12: 开启基于JWT的身份认证

#### 身份认证相关的概念

> 当前Pulsar支持以下身份验证提供者:
>
> - TLS认证
> - Athenz
> - Kerberos
> - JWT认证

#### 开启JWT Token认证

>JWT支持通过两种不同的秘钥生成和验证Token：
>
>- 对称秘钥：
>  - 使用单个Secret key来生成和验证Token
>- 非对称秘钥：包含由私钥和公钥组成的一对密钥
>  - 使用Private key生成Token
>  - 使用Public key验证Token

> 创建秘钥

```bash
./bin/pulsar tokens create-key-pair --output-private-key ./conf/jwt-private.key --output-public-key ./conf/jwt-public.key
```

> 使用 `private-key` 来创建 `admin` 的 `token`

```bash
./bin/pulsar tokens create --private-key ./conf/jwt-private.key --subject admin
```

> 再使用 `private-key` 创建 `test-user` 的 `token`

```bash
./bin/pulsar tokens create --private-key ./conf/jwt-private.key --subject test-user
```

> 现在我们拥有角色 `admin` 和 `test-user`，我们需要对这些角色进行授权。
>
> 下面我们为角色 `test-user` 对 `study/app1` 的 `namespace` 进行 `produce` 以及 `consume` 的授权。

```bash
./bin/pulsar-admin namespaces grant-permission study/app1 --role test-user --actions produce,consume
```

> 现在我们修改 conf 文件来开启权限验证，要让Broker开启认证只需要在`broker.conf`配置文件中添加如下的配置:
>
> 这里需要注意的是，因为我们是 `admin` 用户，所以需要使用 admin 用户的生成的 toekn。
>
> 另外还有一点，如果我们是在本地启动 `standalone`，那么我们应该修改 `./conf/standalone.conf`。

```properties
## 启用认证和鉴权
authenticationEnabled=true
authorizationEnabled=true
authenticationRefreshCheckSeconds=60
authenticationProviders=org.apache.pulsar.broker.authentication.AuthenticationProviderToken

# 设置Broker自身的认证, Broker连接其他Broker时用到
brokerClientAuthenticationPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
brokerClientAuthenticationParameters={"token":"eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.W5h3Y8p9ATSNcQEN9a6v47y5A1ReNHS8gST2B3LwcJfzzFcXtixfIRVdS0AeijsYyeZ14m1ZGvaoULzgqHYIXKqKxNL-g9vfpR_wX3reVplArD2lJsV5ttHzOK0riDi2PHVtvkv9mHIEO6s-AWNZ3vPGpL_EvLtSbSZmiPLN5_wWAwiGgMo1amikDD5ouh5pJYdOB01wMzUrKH_xvhYgWrgIHHmPIG6diDPT4glZ66YdgsAvOOfDLiVxkk7HmeM2P2U4JU6TVgWDxRh7IDeD7uN_o-tq4ayCQ_6db0Axsv4UiVPE4_Tza-ptXMWGdATS0tO-08l_PAnzZDAfx6m0Cg"}

authenticateOriginalAuthData=true

superUserRoles=admin
tokenPublicKey=./conf/jwt-public.key
```

##### 配置命令行工具

> 开启验证之后，我们需要修改 `./conf/client.conf` 为我们的请求增加鉴权。这里我们 `client` 使用的角色还是 `admin`：

```properties
webServiceUrl=http://localhost:8080/
brokerServiceUrl=pulsar://localhost:6650/
authPlugin=org.apache.pulsar.client.impl.auth.AuthenticationToken
authParams={"token":"eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhZG1pbiJ9.W5h3Y8p9ATSNcQEN9a6v47y5A1ReNHS8gST2B3LwcJfzzFcXtixfIRVdS0AeijsYyeZ14m1ZGvaoULzgqHYIXKqKxNL-g9vfpR_wX3reVplArD2lJsV5ttHzOK0riDi2PHVtvkv9mHIEO6s-AWNZ3vPGpL_EvLtSbSZmiPLN5_wWAwiGgMo1amikDD5ouh5pJYdOB01wMzUrKH_xvhYgWrgIHHmPIG6diDPT4glZ66YdgsAvOOfDLiVxkk7HmeM2P2U4JU6TVgWDxRh7IDeD7uN_o-tq4ayCQ_6db0Axsv4UiVPE4_Tza-ptXMWGdATS0tO-08l_PAnzZDAfx6m0Cg"}
```

#### Pulsar的Java和Go客户端

```java
PulsarClient client = PulsarClient.builder()
    .serviceUrl("pulsar://broker.example.com:6650/")
    .authentication(
        AuthenticationFactory.token("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.ZXbMZ4OTXgJFN5tmw8jLd43spaiDYjbd4lmGTLd2_ge0C3V_lVsyv1VSGC5NNn5cik1rkQTS6dRHQ_GzczCCNNdvKr2z5zEFnCdqO8dRtFXliXALMwpno7epctKHa9TvRzozkmYw3zIKCXdjK9WEKkROyiJmqpCrKUOqXiEYH_oFMgBGAacy8VAkjSQlU8BhXGPhnKvLNolH41XRIkHP5EMcxaOC7NPQjDD-oucH2iCnz4AftBhGmredNBzReztZYN0-RvHCS-dwWwwrqO1Zxlnh1BfTSQblEkTMtaG_qiqRNg_gD6CGCp2oJdcv_p2pndgPmgPIuYpIGpgVP_HXGQ")）
    .build();
```

```go
client, err := NewClient(ClientOptions{
    URL:            "pulsar://localhost:6650",
    Authentication: NewAuthenticationToken("eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJ0ZXN0LXVzZXIifQ.ZXbMZ4OTXgJFN5tmw8jLd43spaiDYjbd4lmGTLd2_ge0C3V_lVsyv1VSGC5NNn5cik1rkQTS6dRHQ_GzczCCNNdvKr2z5zEFnCdqO8dRtFXliXALMwpno7epctKHa9TvRzozkmYw3zIKCXdjK9WEKkROyiJmqpCrKUOqXiEYH_oFMgBGAacy8VAkjSQlU8BhXGPhnKvLNolH41XRIkHP5EMcxaOC7NPQjDD-oucH2iCnz4AftBhGmredNBzReztZYN0-RvHCS-dwWwwrqO1Zxlnh1BfTSQblEkTMtaG_qiqRNg_gD6CGCp2oJdcv_p2pndgPmgPIuYpIGpgVP_HXGQ"),
})
```































