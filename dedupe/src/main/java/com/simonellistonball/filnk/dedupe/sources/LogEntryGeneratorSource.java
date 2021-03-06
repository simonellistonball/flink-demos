package com.simonellistonball.filnk.dedupe.sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.simonellistonball.flink.dedupe.models.LogEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.io.IOException;
import java.time.Instant;
import java.util.*;

/**
 * Log Generator that loops a template file and emits random samples
 */
@Slf4j
public class LogEntryGeneratorSource implements ParallelSourceFunction<LogEntry> {
    private Random rnd = new Random();
    private List<LogEntry> entries;
    private int sleepTime = 1000;

    public LogEntryGeneratorSource(List<LogEntry> entries, int sleepTime) {
        this.entries = entries;
        this.sleepTime = sleepTime;
    }

    public static List<LogEntry> readEntriesFromFile(File file) throws IOException {
        ObjectMapper om = new ObjectMapper();
        return Arrays.asList(om.readValue(file, LogEntry[].class));
    }

    @Override
    public void run(SourceContext<LogEntry> sourceContext) throws Exception {
        long l = Instant.now().toEpochMilli();
        LogEntry template = entries.get(rnd.nextInt(entries.size()) - 1);
        LogEntry entry = LogEntry.newBuilder()
                .setData(updateData(template.getData(),l))
                .setTimestamp(l)
                .build();
        sourceContext.collectWithTimestamp(entry, l);
        Thread.sleep(sleepTime);
    }

    private Map<CharSequence, CharSequence> updateData(Map<CharSequence, CharSequence> data, long l) {
        data.put("timestamp", Long.toString(l));
        return data;
    }

    @Override
    public void cancel() {

    }
}
