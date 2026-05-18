package com.onchain;

import com.onchain.config.Configuration;
import com.onchain.service.BlockchainClient;
import com.onchain.service.KafkaProducerService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class App {
    private static final Logger logger = LoggerFactory.getLogger(App.class);

    public static void main(String[] args) {
        logger.info("Starting BTC On-Chain Ingestion Engine...");

        KafkaProducerService processor = new KafkaProducerService(
                Configuration.KAFKA_BOOTSTRAP_SERVERS,
                Configuration.KAFKA_TOPIC);

        BlockchainClient client = new BlockchainClient(Configuration.WS_URL, processor);

        client.start(Configuration.OP_SUBSCRIBE_UNCONFIRMED);
    }
}
