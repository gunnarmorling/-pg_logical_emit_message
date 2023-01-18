/*
 *  Copyright 2021 The original authors
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package dev.morling.demos.logicaldecoding.auditing;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Properties;

import org.apache.flink.api.common.functions.RichFlatMapFunction;
import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.util.Collector;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import dev.morling.demos.logicaldecoding.auditing.model.AuditState;
import dev.morling.demos.logicaldecoding.common.deserialization.MessageDeserializer;
import dev.morling.demos.logicaldecoding.common.model.ChangeEvent;
import dev.morling.demos.logicaldecoding.common.model.Message;

public class AuditMain {

    public static void main(String[] args) throws Exception {
        Properties extraProps = new Properties();
        extraProps.put("poll.interval.ms", "100");
        extraProps.put("snapshot.mode", "never");

        SourceFunction<String> sourceFunction = PostgreSQLSource.<String> builder()
                .hostname("localhost")
                .port(5432)
                .database("demodb")
                .username("postgresuser")
                .password("postgrespw")
                .decodingPluginName("pgoutput")
                .debeziumProperties(extraProps)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(1);
        
        env.addSource(sourceFunction)
                .flatMap(new AuditMetadataEnrichmentFunction())
                .print();
//                .setParallelism(1);

        env.execute();
    }

    public static class AuditMetadataEnrichmentFunction extends RichFlatMapFunction<String, String> implements CheckpointedFunction {

        private static final long serialVersionUID = 1L;

        private transient ObjectMapper mapper;
        private transient ListState<AuditState> auditState;

        public AuditMetadataEnrichmentFunction() {
        }

        public void flatMap(String value, Collector<String> out) throws Exception {
            ChangeEvent changeEvent = mapper.readValue(value, ChangeEvent.class);
            String op = changeEvent.getOp();
            String txId = changeEvent.getSource().get("txId").asText();

            // decoding message
            if (op.equals("m")) {
                Message message = changeEvent.getMessage();

                if (message.getPrefix().equals("audit")) {
                    auditState.update(Arrays.asList(new AuditState(txId, message.getContent())));
                    return;
                }
                else {
                    out.collect(value);
                }
            }
            else {
                AuditState auditState = retrieveAuditState();
                if (txId != null && auditState != null) {
                    if (txId.equals(auditState.getTxId())) {
                        changeEvent.setAuditData(auditState.getState());
                    }
                    else {
                        this.auditState.clear();
                    }
                }

                changeEvent.setTransaction(null);
                out.collect(mapper.writerWithDefaultPrettyPrinter().writeValueAsString(changeEvent));
            }
        }
        
        private AuditState retrieveAuditState() throws Exception {
            Iterable<AuditState> state = auditState.get();
            
            if (state == null) {
                return null;
            }
            
            Iterator<AuditState> it = state.iterator();
            if (!it.hasNext()) {
                return null;
            }
            
            return it.next();
        }

        @Override
        public void snapshotState(FunctionSnapshotContext context) throws Exception {
            
        }

        @Override
        public void initializeState(FunctionInitializationContext context) throws Exception {
            mapper = new ObjectMapper();
            SimpleModule module = new SimpleModule();
            module.addDeserializer(Message.class, new MessageDeserializer());
            mapper.registerModule(module);

            ListStateDescriptor<AuditState> descriptor = new ListStateDescriptor<>("auditState", AuditState.class);
            auditState = context.getOperatorStateStore().getListState(descriptor);
        }
    }
}
