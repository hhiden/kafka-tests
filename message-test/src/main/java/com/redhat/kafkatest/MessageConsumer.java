package com.redhat.kafkatest;

import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.clients.consumer.Consumer;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.ByteArrayDeserializer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Pulls messages from the queue
 *
 * @author hhiden
 */
public class MessageConsumer {

    private final static String TOPIC = "output-data";
    private final static String BOOTSTRAP_SERVERS = "localhost:9092";

    public static Consumer<Long, byte[]> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "KafkaExampleConsumer");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class.getName());
        final Consumer<Long, byte[]> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    static void runConsumer() throws InterruptedException {
        final Consumer<Long, byte[]> consumer = createConsumer();
        final int giveUp = 100;
        int noRecordsCount = 0;
        while (true) {
            final ConsumerRecords<Long, byte[]> consumerRecords
                    = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) {
                    break;
                } else {
                    continue;
                }
            }
            consumerRecords.forEach(record -> {
                try {
                    System.out.printf("Consumer Record:(%d, %s, %d, %d)\n",
                            record.key(), SerializationUtils.deserialize(record.value()).toString(),
                            record.partition(), record.offset());
                } catch (Exception e){
                    e.printStackTrace();
                }
            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("DONE");
    }
    
    public static void main(String[] args){
        try {
            runConsumer();
        } catch (Exception e){
            e.printStackTrace();
        }
    }
}
