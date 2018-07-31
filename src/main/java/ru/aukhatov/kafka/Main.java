package ru.aukhatov.kafka;

import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class Main {

    public static void main(String[] args) {

        final String mobileGroupId = "mobile";
        final String webGroupId = "web";
        final String topic = "news";
        final String kafkaAddress = "localhost:9092";

        ScheduledExecutorService executorService = Executors.newScheduledThreadPool(4);
        SimpleProducer producer = new SimpleProducer(topic, kafkaAddress);

        try {
            executorService.submit(new SimpleConsumer(topic, mobileGroupId, "mobile-app", kafkaAddress));
            executorService.submit(new SimpleConsumer(topic, webGroupId, "web-app", kafkaAddress));
            producer.execute();
        } catch (Exception ex) {
            ex.printStackTrace();
        } finally {
            executorService.shutdown();
        }
    }
}
