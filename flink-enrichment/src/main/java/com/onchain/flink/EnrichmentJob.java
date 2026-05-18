package com.onchain.flink;

import com.onchain.flink.config.Configuration;
import com.onchain.flink.mapper.EnrichmentMapper;
import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.connector.base.DeliveryGuarantee;
import org.apache.flink.connector.kafka.sink.KafkaRecordSerializationSchema;
import org.apache.flink.connector.kafka.sink.KafkaSink;
import org.apache.flink.connector.kafka.source.KafkaSource;
import org.apache.flink.connector.kafka.source.enumerator.initializer.OffsetsInitializer;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnrichmentJob {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentJob.class);

    public static void main(String[] args) throws Exception {
        logger.info("Starting enrichment job. bootstrapServers={}, inputTopic={}, outputTopic={}",
                Configuration.BOOTSTRAP_SERVERS, Configuration.INPUT_TOPIC, Configuration.OUTPUT_TOPIC);

        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

        KafkaSource<String> source = KafkaSource.<String>builder()
                .setBootstrapServers(Configuration.BOOTSTRAP_SERVERS)
                .setTopics(Configuration.INPUT_TOPIC)
                .setGroupId(Configuration.CONSUMER_GROUP_ID)
                .setStartingOffsets(OffsetsInitializer.earliest())
                .setValueOnlyDeserializer(new SimpleStringSchema())
                .build();

        DataStream<String> input = env.fromSource(
                source,
                WatermarkStrategy.noWatermarks(),
                "kafka-source-btc-mempool"
        );

        DataStream<String> enriched = input
                .map(new EnrichmentMapper())
                .name("enrichment-mapper");

        KafkaSink<String> sink = KafkaSink.<String>builder()
                .setBootstrapServers(Configuration.BOOTSTRAP_SERVERS)
                .setRecordSerializer(
                        KafkaRecordSerializationSchema.builder()
                                .setTopic(Configuration.OUTPUT_TOPIC)
                                .setValueSerializationSchema(new SimpleStringSchema())
                                .build()
                )
                .setDeliveryGuarantee(DeliveryGuarantee.AT_LEAST_ONCE)
                .build();

        enriched.sinkTo(sink).name("kafka-sink-btc-tx-enriched");

        env.execute("BTC Tx Enrichment");
    }
}
