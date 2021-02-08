package com.simonellistonball.filnk.dedupe.sources;

import com.simonellistonball.flink.dedupe.Config;
import com.simonellistonball.flink.dedupe.KafkaUtils;
import com.simonellistonball.flink.dedupe.models.LogEntry;
import com.simonellistonball.flink.dedupe.LogEntrySchema;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer;

import java.util.Properties;

public class InputSource {

    public static SourceFunction<LogEntry> getLogStream(Config config) {
        Properties kafkaProps = KafkaUtils.initConsumerProperties(config);
        String inputTopic = config.getInputTopic();
        FlinkKafkaConsumer<LogEntry> kafkaConsumer = new FlinkKafkaConsumer<>(inputTopic, new LogEntrySchema(), kafkaProps);
        kafkaConsumer.setStartFromLatest();
        return kafkaConsumer;
    }
}
