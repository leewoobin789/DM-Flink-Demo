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

package de.fom.woobin.datamesh;

import java.util.Properties;

import org.apache.flink.api.common.RuntimeExecutionMode;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Schema;
import org.apache.flink.table.api.Slide;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;

import de.fom.woobin.datamesh.customerretention.objects.clickstream.CustomerClickStream;
import de.fom.woobin.datamesh.customerretention.objects.clickstream.CustomerClickStreamDeserializationSchema;

import static org.apache.flink.table.api.Expressions.$;
import static org.apache.flink.table.api.Expressions.*;

public class CustomerClickStreamAggregation {

	public static void main(String[] args) throws Exception {
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setRuntimeMode(RuntimeExecutionMode.STREAMING);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		tEnv.getConfig().getConfiguration().setString("table.exec.source.idle-timeout", "1000");
		Properties kafkaProps = new Properties();
        kafkaProps.put("transaction.timeout.ms", 600000);

		KafkaSource<CustomerClickStream> source = KafkaSource.<CustomerClickStream>builder()
			.setBootstrapServers(Constants.BROKER)
			.setTopics(Constants.CustomerRetention.CUSTOMER_CLICK_STREAM_TOPIC)
			.setGroupId(Constants.CustomerRetention.CONSUMER_GROUP)
			.setStartingOffsets(OffsetsInitializer.earliest())
			.setValueOnlyDeserializer(new CustomerClickStreamDeserializationSchema())
			.build();

		DataStream<CustomerClickStream> stream = env
			.fromSource(source, WatermarkStrategy.noWatermarks(), "Kafka Source");

		Table tmpTable = tEnv.fromDataStream(stream);

		Table result = tEnv.fromDataStream(
				stream,
				Schema.newBuilder()
					.fromResolvedSchema(tmpTable.getResolvedSchema())
					.columnByExpression("event_time", "TO_TIMESTAMP_LTZ(ts,3)")
            		.watermark("event_time", "event_time - INTERVAL '1' SECOND")
					.build()
			)
			.window(
				Slide.over(lit(1).minutes()).every(lit(30).seconds()).on($("event_time")).as("w")
			)
			.groupBy($("itemOfPage"),$("w"))
			.select(
				$("itemOfPage").as("itemId"),
				$("w").end().toTimestamp().as("aggregationEnd"),
				$("itemOfPage").count().as("countStreamAggregate")
			);

		tEnv.executeSql(String.format(
			"CREATE TABLE clickstream_aggregate (\n" +
			"    itemId STRING NOT NULL,\n" +
			"    aggregationEnd TIMESTAMP,\n" +
			"    countStreamAggregate     BIGINT\n" +
			") WITH (\n" +
			"    'connector' = 'kafka',\n" +
			"    'topic'     = '%s',\n" +
			"    'key.format'     = 'raw',\n" +
			"    'key.fields'     = 'itemId',\n" +
			"    'properties.bootstrap.servers' = 'localhost:9092',\n" +
			"    'value.format'    = 'json'\n" +
			")", Constants.CustomerRetention.CUSTOMER_CLICK_STREAM_AGGREGATE_TOPIC));

		result.executeInsert("clickstream_aggregate");
	}
}
