package com.simonellistonball.flink.dedupe;

import java.util.Properties;

public class KafkaUtils {

    public static Properties initConsumerProperties(Config config) {
        Properties kafkaProps = initProperties(config);
        String offset = config.getOffset();
        kafkaProps.setProperty("auto.offset.reset", offset);
        return kafkaProps;
    }

    public static Properties initProducerProperties(Config params) {
        return initProperties(params);
    }

    private static Properties initProperties(Config config) {
        Properties kafkaProps = new Properties();
        String servers = config.getBootstrapServers();
        kafkaProps.setProperty("bootstrap.servers", servers);
        return kafkaProps;
    }
}