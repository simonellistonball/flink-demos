package com.simonellistonball.flink.dedupe.sinks;

import com.simonellistonball.flink.dedupe.Config;
import com.simonellistonball.flink.dedupe.KafkaUtils;
import com.simonellistonball.flink.dedupe.models.LogEntry;
import com.simonellistonball.flink.dedupe.LogEntrySchema;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class OutputSink {

    public static SinkFunction<LogEntry> getKafkaOutputSink(Config config) {
        Properties kafkaProps = KafkaUtils.initProducerProperties(config);
        String outputTopic = config.getOutputTopic();
        return new FlinkKafkaProducer<LogEntry>(outputTopic, new LogEntrySchema(), kafkaProps, FlinkKafkaProducer.Semantic.EXACTLY_ONCE, 10);
    }
}
