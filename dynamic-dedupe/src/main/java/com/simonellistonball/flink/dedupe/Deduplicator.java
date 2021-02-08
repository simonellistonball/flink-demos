/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.simonellistonball.flink.dedupe;

import com.simonellistonball.flink.dedupe.functions.DuplicateWindowAggregationFunction;
import com.simonellistonball.flink.dedupe.functions.DuplicationKeyFunction;
import com.simonellistonball.flink.dedupe.functions.DuplicationRuleFunction;
import com.simonellistonball.flink.dedupe.models.DeduplicationRule;
import com.simonellistonball.flink.dedupe.models.LogEntry;
import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.streaming.api.datastream.BroadcastStream;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.assigners.EventTimeSessionWindows;
import org.apache.flink.streaming.api.windowing.time.Time;

import static com.simonellistonball.flink.dedupe.sinks.OutputSink.getKafkaOutputSink;
import static com.simonellistonball.flink.dedupe.sources.ConfigSource.getRulesUpdateStream;
import static com.simonellistonball.flink.dedupe.sources.InputSource.getLogStream;

@Slf4j
@AllArgsConstructor
@NoArgsConstructor
public class Deduplicator {

    private Config config;

    public void run(ParameterTool params) throws Exception {

        this.config = new Config(params);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        DataStream<DeduplicationRule> rulesUpdateStream = env.addSource(getRulesUpdateStream(config)).name("Config Source");
        DataStream<LogEntry> logEntries = env.addSource(getLogStream(config)).name("Log Source");

        BroadcastStream<DeduplicationRule> keysStream = rulesUpdateStream.broadcast(Descriptors.keysDescriptor);
        BroadcastStream<DeduplicationRule> rulesStream = rulesUpdateStream.broadcast(Descriptors.rulesDescriptor);

        // Processing pipeline setup
        DataStream<LogEntry> outputEntries =
                logEntries
                        .connect(keysStream)
                        .process(new DuplicationKeyFunction())
                        .uid("DuplicationKeyFunction")
                        .name("Dynamic Partitioning Function")
                        .keyBy((keyed) -> keyed.getKey())
                        .window(EventTimeSessionWindows.withGap(Time.seconds(60)))
                        .process(new DuplicateWindowAggregationFunction())
                        .uid("DuplicateWindowAggregationFunction")
                        .name("Duplcation Window Processor");

        outputEntries.addSink(getKafkaOutputSink(config));

        env.execute("Flink Log Deduplication");
    }

    public static class Descriptors {
        public static final MapStateDescriptor<Integer, DeduplicationRule> keysDescriptor =
                new MapStateDescriptor<>(
                        "keys", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(DeduplicationRule.class));

        public static final MapStateDescriptor<Integer, DeduplicationRule> rulesDescriptor =
                new MapStateDescriptor<>(
                        "rules", BasicTypeInfo.INT_TYPE_INFO, TypeInformation.of(DeduplicationRule.class));
    }
}
