package ru.aukhatov.kafka;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

public class SimpleConsumer implements Runnable {

    private static final long POLL_TIMEOUT = 100L;
    private KafkaConsumer<String, String> consumer;
    private String clientId;
    private String kafkaAddress;

    public SimpleConsumer(String topicName, String groupId, String clientId, String kafkaAddress) {
        this.clientId = clientId;
        this.kafkaAddress = kafkaAddress;

        Properties props = buildConsumerProperties();
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.CLIENT_ID_CONFIG, clientId);
        consumer = new KafkaConsumer<>(props, new StringDeserializer(), new StringDeserializer());

        consumer.subscribe(Collections.singletonList(topicName));

        System.out.printf("Subscribed to topic = %s, group = %s, clientId = %s\n", topicName, groupId, clientId);
    }

    private Properties buildConsumerProperties() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        return props;
    }

    @Override
    public void run() {
        try {
            while (true) {
                consumer.poll(POLL_TIMEOUT)
                        .forEach(record -> {
                            System.out.printf("offset = %d, key = %s, value = %s,  time = %s, clientId = %s, thread = %s \n",
                                    record.offset(), record.key(), record.value(), Instant.now(), clientId, Thread.currentThread().getName());
                        });

            }
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            consumer.close();
        }

    }
}
