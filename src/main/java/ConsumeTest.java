import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;

import java.util.Arrays;
import java.util.Properties;

/**
 * 以下是kafka消息消费案例
 */
public class ConsumeTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 设置kafka服务地址
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 指定消费者组（避免多节点同时消费一个topic的重复消费的问题）
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test");
        // 开启自动提交配置
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);
        // 自动提交时间间隔
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, 1000);

        // 心跳时间检测间隔
        props.put(ConsumerConfig.HEARTBEAT_INTERVAL_MS_CONFIG, 6000);
        // 消费者组失效超时时间
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, 28000);
        // 消费者组位移丢失和越界后恢复起始位置
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "latest");

        // 最大拉取时间间隔
        props.put(ConsumerConfig.MAX_POLL_INTERVAL_MS_CONFIG, 300000);
        // 一次最大拉取的量
        props.put(ConsumerConfig.MAX_POLL_RECORDS_CONFIG, 1000);

        // 设置key-value反序列化器
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());

        KafkaConsumer<byte[], byte[]> consumer = new KafkaConsumer(props);
        consumer.subscribe(Arrays.asList("test"));

        // 通过死循环持续消费消息
        while (true) {
            try {
                // 拉取数据
                // 这里的1000表示拉取超时时间，单位是毫秒，意思是等待1000毫秒，如果1000毫秒内没有拉取到数据就返回了
                consumer.poll(1000).forEach(record -> {
                    System.out.println("topic:" + record.topic()
                            + " partition:" + record.partition()
                            + " offset:" + record.offset()
                            + " key:" + new String(record.key())
                            + " value:" + new String(record.value()));
                    // 以下添加数据消费逻辑
                    // 需要注意的是，如果消费耗时大于配置的最大拉取时间间隔配置（）max.poll.interval.ms），会导致消费组下线，和重复消费

                });
                // 提交确认消费，如果未开启自动提交（ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG），需要手动提交确认消费
                // consumer.commitSync();
            }catch (Exception e){
                e.printStackTrace();
            }
        }
    }
}
