package com.simonellistonball.filnk.dedupe.sources;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.simonellistonball.flink.dedupe.models.LogEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.functions.source.ParallelSourceFunction;

import java.io.File;
import java.time.Instant;
import java.util.*;

/**
 * Log Generator that loops a template file and emits random samples
 */
@Slf4j
public class LogEntryGeneratorSource implements ParallelSourceFunction<LogEntry> {
    private Random rnd = new Random();
    private List<LogEntry> entries;
    private long sleepTime = 1000;

    public LogEntryGeneratorSource(ParameterTool params) {
        File file = new File(params.get("input"));
        sleepTime = params.getInt("generator.sleeptime");
        ObjectMapper om = new ObjectMapper();
        try {
            entries = Arrays.asList(om.readValue(file, LogEntry[].class));
        } catch (Exception e) {
            log.error("Template initialisation failed", e);
        }
    }

    @Override
    public void run(SourceContext<LogEntry> sourceContext) throws Exception {
        long l = Instant.now().toEpochMilli();

        LogEntry template = entries.get(rnd.nextInt(entries.size()) - 1);
        LogEntry entry = LogEntry.builder().data(updateData(template.getData(),l)).timestamp(l).build();
        Thread.sleep(sleepTime);
        sourceContext.collectWithTimestamp(entry, l);
    }

    private HashMap<String, String> updateData(HashMap<String, String> data, long l) {
        data.put("timestamp", Long.toString(l));
        return data;
    }

    @Override
    public void cancel() {

    }
}
