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

import org.apache.flink.api.common.state.BroadcastState;
import org.apache.flink.streaming.api.functions.co.BroadcastProcessFunction;
import org.apache.flink.util.Collector;

public class DuplicationKeyFunction extends BroadcastProcessFunction<LogEntry, DeduplicationRule, Keyed<LogEntry, String, Integer>> {

    @Override
    public void processElement(LogEntry logEntry, ReadOnlyContext readOnlyContext, Collector<Keyed<LogEntry, String, Integer>> collector) throws Exception {

    }

    @Override
    public void processBroadcastElement(DeduplicationRule deduplicationRule, Context context, Collector<Keyed<LogEntry, String, Integer>> collector) throws Exception {
        BroadcastState<Integer, DeduplicationRule> broadcastState =
                context.getBroadcastState(Deduplicator.Descriptors.keysDescriptor);

    }
}
