package com.simonellistonball.flink.dedupe.functions;

import com.simonellistonball.flink.dedupe.models.Keyed;
import com.simonellistonball.flink.dedupe.models.LogEntry;
import org.apache.flink.streaming.api.functions.KeyedProcessFunction;
import org.apache.flink.streaming.api.functions.windowing.ProcessWindowFunction;
import org.apache.flink.streaming.api.windowing.windows.TimeWindow;
import org.apache.flink.util.Collector;
import sun.rmi.runtime.Log;

public class DuplicateWindowAggregationFunction
        extends ProcessWindowFunction<Keyed<LogEntry, String, Integer>, LogEntry, String, TimeWindow> {


    @Override
    public void process(String key,
                        Context context,
                        Iterable<Keyed<LogEntry, String, Integer>> entries,
                        Collector<LogEntry> out) {

        out.collect();

    }
}
