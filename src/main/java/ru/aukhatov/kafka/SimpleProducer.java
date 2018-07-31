package ru.aukhatov.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.Properties;

public class SimpleProducer {

    private static final String DELIMITER = ":";
    private static final String EXIT_KEY = "q";
    private final String topicName;
    private KafkaProducer producer;
    private String kafkaAddress;

    public SimpleProducer(String topicName, String kafkaAddress) {
        this.topicName = topicName;
        this.kafkaAddress = kafkaAddress;
        this.producer = new KafkaProducer(buildProducerProperties(), new StringSerializer(), new StringSerializer());
        System.out.printf("Producer topic %s initialized.\n", topicName);
    }

    public void execute() throws IOException {
        consoleReader();
    }

    private void consoleReader() throws IOException {
        try (BufferedReader br = new BufferedReader(new InputStreamReader(System.in))) {

            while (true) {
                String input = br.readLine();
                String[] split = input.split(DELIMITER);

                if (EXIT_KEY.equals(input)) {
                    producer.close();
                    System.out.println("Exit!");
                    System.exit(0);
                } else if (!input.isEmpty()) {
                    chooseStrategy(split);
                }
            }
        }
    }

    private void chooseStrategy(final String[] split) {
        switch (split.length) {
            case 1:
                // strategy by round robin
                producer.send(new ProducerRecord(topicName, split[0]));
                break;
            case 2:
                // strategy by hash
                producer.send(new ProducerRecord(topicName, split[0], split[1]));
                break;
            case 3:
                // strategy by partition
                producer.send(new ProducerRecord(topicName, Integer.valueOf(split[2]), split[0], split[1]));
                break;
            default:
                System.out.println("Enter key:value, q - Exit");
        }
    }

    private Properties buildProducerProperties() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaAddress);
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.LINGER_MS_CONFIG, 1);
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432);
        return props;
    }
}
