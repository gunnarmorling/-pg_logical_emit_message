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
package dev.morling.demos.logicaldecoding;

import java.util.Properties;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.ValueState;
import org.apache.flink.api.common.state.ValueStateDescriptor;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.module.SimpleModule;
import com.ververica.cdc.connectors.postgres.PostgreSQLSource;
import com.ververica.cdc.debezium.JsonDebeziumDeserializationSchema;

import dev.morling.demos.logicaldecoding.deserialization.MessageDeserializer;
import dev.morling.demos.logicaldecoding.model.AuditState;
import dev.morling.demos.logicaldecoding.model.ChangeEvent;
import dev.morling.demos.logicaldecoding.model.Message;

public class Main {

    public static void main(String[] args) throws Exception {
        Properties extraProps = new Properties();
        extraProps.put("poll.interval.ms", "100");

        SourceFunction<String> sourceFunction = PostgreSQLSource.<String> builder()
                .hostname("localhost")
                .port(5432)
                .database("orderdb")
                .username("postgresuser")
                .password("postgrespw")
                .decodingPluginName("pgoutput")
                .debeziumProperties(extraProps)
                .deserializer(new JsonDebeziumDeserializationSchema()) // converts SourceRecord to JSON String
                .build();

        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        env.addSource(sourceFunction)
                .keyBy(v -> 1L)
                .map(new AuditMetadataEnrichmentFunction())
                .print()
                .setParallelism(1);

        env.execute();
    }

    public static class AuditMetadataEnrichmentFunction extends RichMapFunction<String, String> {

        private static final long serialVersionUID = 1L;

        private final ObjectMapper mapper;

        private transient ValueState<AuditState> auditState;

        public AuditMetadataEnrichmentFunction() {
            mapper = new ObjectMapper();
            SimpleModule module = new SimpleModule();
            module.addDeserializer(Message.class, new MessageDeserializer());
            mapper.registerModule(module);
        }

        @Override
        public String map(String value) throws Exception {
            ChangeEvent changeEvent = mapper.readValue(value, ChangeEvent.class);
            String op = changeEvent.getOp();
            String txId = changeEvent.getSource().get("txId").asText();

            if (op.equals("m")) {
                Message message = changeEvent.getMessage();
                auditState.update(new AuditState(txId, message.getContent()));
                return value;
            }
            else {
                if (txId != null && auditState.value() != null) {
                    if (txId.equals(auditState.value().getTxId())) {
                        changeEvent.setAuditData(auditState.value().getState());
                    }
                    else {
                        auditState.clear();
                    }
                }

                changeEvent.setTransaction(null);
                return mapper.writerWithDefaultPrettyPrinter().writeValueAsString(changeEvent);
            }
        }

        @Override
        public void open(Configuration parameters) throws Exception {
            ValueStateDescriptor<AuditState> descriptor = new ValueStateDescriptor<>("auditState", AuditState.class);
            auditState = getRuntimeContext().getState(descriptor);
        }
    }
}
