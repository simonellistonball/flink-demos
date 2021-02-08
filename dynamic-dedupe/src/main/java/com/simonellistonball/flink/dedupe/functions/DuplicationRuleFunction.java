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
package com.simonellistonball.flink.dedupe.functions;

import com.simonellistonball.flink.dedupe.Deduplicator;
import com.simonellistonball.flink.dedupe.models.DeduplicationRule;
import com.simonellistonball.flink.dedupe.models.Keyed;
import com.simonellistonball.flink.dedupe.models.LogEntry;
import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.api.common.state.ReadOnlyBroadcastState;
import org.apache.flink.streaming.api.functions.co.KeyedBroadcastProcessFunction;
import org.apache.flink.util.Collector;

import java.util.Map;

@Slf4j
public class DuplicationRuleFunction extends KeyedBroadcastProcessFunction<String, Keyed<LogEntry, String, Integer>, DeduplicationRule, LogEntry> {

    @Override
    public void processElement(Keyed<LogEntry, String, Integer> keyedLogEntry, ReadOnlyContext readOnlyContext, Collector<LogEntry> collector) throws Exception {
        ReadOnlyBroadcastState<Integer, DeduplicationRule> rulesState =
                readOnlyContext.getBroadcastState(Deduplicator.Descriptors.rulesDescriptor);
        rulesState.immutableEntries().forEach(r -> {
            processRule(r.getValue(), keyedLogEntry);
        });
    }

    private LogEntry processRule(DeduplicationRule rule, Keyed<LogEntry, String, Integer> keyedLogEntry) {

        keyedLogEntry.getWrapped().getData();
    }

    @Override
    public void processBroadcastElement(DeduplicationRule deduplicationRule, Context context, Collector<LogEntry> collector) throws Exception {
        BroadcastState<Integer, DeduplicationRule> broadcastState =
                context.getBroadcastState(Deduplicator.Descriptors.rulesDescriptor);
        broadcastState.put(deduplicationRule.getId(), deduplicationRule);
    }
}
