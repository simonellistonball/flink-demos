package com.simonellistonball.filnk.dedupe;

import com.simonellistonball.filnk.dedupe.sources.LogEntryGeneratorSource;
import com.simonellistonball.flink.dedupe.models.LogEntry;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer;

import java.io.File;

public class DataGeneratorJob {
    public static void main(String[] args) throws Exception {
        if (args.length != 1) {
            throw new RuntimeException("Path to the properties file is expected as the only argument.");
        }
        ParameterTool params = ParameterTool.fromPropertiesFile(args[0]);

        Config config = new Config(params);

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        DataStream<LogEntry> generatedInput =
                env.addSource(new LogEntryGeneratorSource(
                        LogEntryGeneratorSource.readEntriesFromFile(config.getInputFile()),
                        config.getSleepTime()))
                        .name("Log Entry Generator");

        FlinkKafkaProducer<LogEntry> kafkaSink = new FlinkKafkaProducer<LogEntry>(config.getInputTopic(),
                Utils.readKafkaProperties(params, false),
                FlinkKafkaProducer.Semantic.EXACTLY_ONCE,
                1);

        generatedInput.keyBy("itemId").addSink(kafkaSink).name("Transaction Kafka Sink");
        env.execute("Kafka Data generator");
    }

}
