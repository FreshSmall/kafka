/*
 * Copyright (C) GSX Techedu Inc. All Rights Reserved
 * Unauthorized copying of this file, via any medium is strictly prohibited
 * Proprietary and confidential
 */

package demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.util.Properties;
import java.util.Random;
import java.util.concurrent.BrokenBarrierException;
import java.util.concurrent.CyclicBarrier;

/**
 * @author yinchao
 * @description
 * @team wuhan operational dev.
 * @date 2021/10/9 11:45
 **/
public class ProducerDemo {

    private static final String TOPIC = "kafkaSourceCode"; //kafka创建的topic
    private static final String CONTENT = "This is a single message"; //要发送的内容
    private static final String BROKER_LIST = "127.0.0.1:9092"; //broker的地址和端口
    private static final String SERIALIZER_CLASS = "kafka.serializer.StringEncoder"; // 序列化类

    private static CyclicBarrier cyclicBarrier;
    private static KafkaProducer<Integer, String> producer;

    static class CycliBarrierThread implements Runnable {

        @Override
        public void run() {
            final String threadName = Thread.currentThread().getName();
            int j = new Random(1000).nextInt();
            ProducerRecord<Integer, String> message = new ProducerRecord<>(TOPIC, j, CONTENT + j);
            try {
                cyclicBarrier.await();
            } catch (InterruptedException e) {
                e.printStackTrace();
            } catch (BrokenBarrierException e) {
                e.printStackTrace();
            }
            producer.send(message, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception exception) {
                    System.out.println("数据插入完成，线程名称：" + threadName);
                }
            });
        }
    }

    public static void main(String[] args) throws BrokenBarrierException, InterruptedException {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BROKER_LIST);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 1);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
//        List<String> interceptors = new ArrayList<>();
//        interceptors.add("kafka.examples.mime.interceptor.ProducerInterceptorDemo");
//        props.put(ProducerConfig.INTERCEPTOR_CLASSES_CONFIG, interceptors);

        producer = new KafkaProducer<>(props);
        cyclicBarrier = new CyclicBarrier(7, new Runnable() {
            @Override
            public void run() {
                System.out.println("所有线程开始发送数据");
            }
        });

        for (int i = 0; i < 6; i++) {
            new Thread(new CycliBarrierThread()).start();
        }
        cyclicBarrier.await();
        Thread.sleep(1000 * 5);
        producer.close();
    }

}
