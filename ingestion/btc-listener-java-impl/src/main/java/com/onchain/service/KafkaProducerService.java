package com.onchain.service;

import com.fasterxml.jackson.databind.JsonNode;
import com.onchain.config.Configuration;
import com.onchain.util.JsonUtils;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;
import java.util.concurrent.ExecutionException;

public class KafkaProducerService {
    private static final Logger logger = LoggerFactory.getLogger(KafkaProducerService.class);
    private final KafkaProducer<String, String> producer;
    private final String topic;

    public KafkaProducerService(String bootstrapServers, String topic) {
        this.topic = topic;

        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        props.put(ProducerConfig.CLIENT_ID_CONFIG, Configuration.KAFKA_CLIENT_ID);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.LINGER_MS_CONFIG, "0");

        logger.info("Initializing Kafka Producer to: {}", bootstrapServers);
        this.producer = new KafkaProducer<>(props);
    }

    public void process(String rawJsonMessage) {
        JsonUtils.parse(rawJsonMessage).ifPresent(node -> {
            String op = node.has("op") ? node.get("op").asText() : "unknown";

            if (Configuration.OP_RESPONSE_UTX.equals(op)) {
                JsonNode xNode = node.get("x");
                String txHash = (xNode != null && xNode.has("hash")) ? xNode.get("hash").asText() : null;

                if (txHash != null) {
                    sendToKafka(txHash, rawJsonMessage);
                }
            }
        });
    }

    private void sendToKafka(String key, String value) {
        ProducerRecord<String, String> record = new ProducerRecord<>(topic, key, value);
        try {
            producer.send(record).get();
        } catch (InterruptedException | ExecutionException e) {
            logger.error("Error sending to Kafka", e);
        }
    }
}
