package com.simonellistonball.flink.dedupe.sources;

import com.simonellistonball.flink.dedupe.*;
import com.simonellistonball.flink.dedupe.models.DeduplicationRule;
import org.apache.flink.formats.avro.registry.cloudera.ClouderaRegistryKafkaDeserializationSchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;

import java.util.Properties;

public class ConfigSource {
    public static SourceFunction<DeduplicationRule> getRulesUpdateStream(Config config) {
        KafkaDeserializationSchema<DeduplicationRule> schema = ClouderaRegistryKafkaDeserializationSchema
                .builder(DeduplicationRule.class)
                .setConfig(config.getSchemaRegistryProperties())
                .build();

        Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
        String inputTopic = config.getInputTopic();
        FlinkKafkaConsumer<DeduplicationRule> kafkaConsumer = new FlinkKafkaConsumer<>(inputTopic, schema, kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
    }
}
