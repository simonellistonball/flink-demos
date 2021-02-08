package com.simonellistonball.filnk.dedupe;

import com.simonellistonball.flink.dedupe.models.LogEntry;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.connectors.kafka.KafkaDeserializationSchema;
import org.apache.flink.streaming.connectors.kafka.KafkaSerializationSchema;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.codehaus.jackson.map.ObjectMapper;

import javax.annotation.Nullable;

public class LogEntrySchema implements KafkaSerializationSchema<LogEntry>, KafkaDeserializationSchema<LogEntry> {

    private ObjectMapper om = new ObjectMapper();
    private String topic;

    @Override
    public ProducerRecord<byte[], byte[]> serialize(LogEntry logEntry, @Nullable Long timestamp) {
        try {
            return new ProducerRecord(topic, om.writeValueAsBytes(logEntry));
        } catch (Exception e) {
            return null;
        }

    }

    @Override
    public boolean isEndOfStream(LogEntry logEntry) {
        return false;
    }

    @Override
    public LogEntry deserialize(ConsumerRecord<byte[], byte[]> consumerRecord) throws Exception {
        LogEntry entry = new LogEntry();
        entry.setData(null);
        return om.readValue(consumerRecord.value(), LogEntry.class);
    }

    @Override
    public TypeInformation<LogEntry> getProducedType() {
        return TypeInformation.of(LogEntry.class);
    }
}
