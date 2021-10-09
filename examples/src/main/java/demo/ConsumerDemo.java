/*
 * Copyright (C) GSX Techedu Inc. All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

package demo;

import kafka.examples.KafkaProperties;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.Collections;
import java.util.Properties;

/**
 * @author yinchao
 * @description
 * @team wuhan operational dev.
 * @date 2021/10/9 11:45
 **/
public class ConsumerDemo {

    private static final String TOPIC = "kafkaSourceCode"; //kafka创建的topic
    private static final String BROKER_LIST = "localhost:9092"; //broker的地址和端口
    private static final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder"; // 序列化类
    private static KafkaConsumer<Integer, String> consumer;

    public static void main(String[] args) {
        String groupId = "DemoConsumer";
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.IntegerDeserializer");
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG,
            "org.apache.kafka.common.serialization.StringDeserializer");
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        consumer = new KafkaConsumer<>(props);

        consumer.subscribe(Collections.singletonList(TOPIC));

        ConsumerRecords<Integer, String> records = consumer.poll(1000);
        for (ConsumerRecord<Integer, String> record : records) {
            System.out.println(
                "Received message: (" + record.key() + ", " + record.value() + ") at offset "
                    + record.offset());
        }
    }

}
