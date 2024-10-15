import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;

/**
 * 以下是kafka消息生产案例
 */
public class ProductTest {
    public static void main(String[] args) {
        Properties props = new Properties();
        // 设置kafka服务地址
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        // 设置key-value序列化器
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // 创建生产对象
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);
        // 发送消息
        producer.send(new ProducerRecord<>("test", "hello kafka"));
        producer.flush();
        producer.close();
    }
}
