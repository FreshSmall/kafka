package demo;

import kafka.examples.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;

import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Properties;

public class ConsumerDemo {
    private static final String TOPIC = "kafkaSourceCode"; //kafka创建的topic
    private static final String BROKER_LIST = "127.0.0.1:9092"; //broker的地址和端口
    private static final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder"; // 序列化类
    private static int messageRemaining;
    private static KafkaConsumer<Integer, String> consumer;

    public static void main(String[] args) {
        String groupId = "DemoConsumer";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaProperties.KAFKA_SERVER_URL + ":" + KafkaProperties.KAFKA_SERVER_PORT);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        System.out.println(consumer.listTopics().toString());
        for (int i = 0; i < 2; i++) {
            ConsumerRecords<Integer, String> records = consumer.poll(Duration.ofSeconds(1));
            for (ConsumerRecord<Integer, String> record : records) {
                System.out.println(groupId + " received message : from partition " + record.partition() + ", (" + record.key() + ", " + record.value() + ") at offset " + record.offset());
            }
            messageRemaining -= records.count();
            if (messageRemaining <= 0) {
                System.out.println(groupId + " finished reading messages");
            }
        }
        System.out.println(consumer.listTopics().toString());
    }
}
