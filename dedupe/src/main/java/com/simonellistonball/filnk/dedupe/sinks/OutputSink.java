package com.simonellistonball.filnk.dedupe.sinks;

import com.simonellistonball.filnk.dedupe.Config;
import com.simonellistonball.filnk.dedupe.KafkaUtils;
import com.simonellistonball.filnk.dedupe.LogEntrySchema;
import com.simonellistonball.flink.dedupe.models.LogEntry;
import org.apache.flink.streaming.api.functions.sink.SinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.util.Properties;

public class OutputSink {

    public static SinkFunction<LogEntry> getKafkaOutputSink(Config config) {
        Properties kafkaProps = KafkaUtils.initProducerProperties(config);
        String outputTopic = config.getOutputTopic();
        return new FlinkKafkaProducer<LogEntry>(
                outputTopic,
                new LogEntrySchema(),
                kafkaProps,
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE,
                10);
    }
}
