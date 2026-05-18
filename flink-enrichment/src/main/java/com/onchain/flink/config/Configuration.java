package com.onchain.flink.config;

public final class Configuration {
    public static final String INPUT_TOPIC = "btc-mempool";
    public static final String OUTPUT_TOPIC = "btc-tx-enriched";
    public static final String BOOTSTRAP_SERVERS = "kafka:29092";
    public static final String CONSUMER_GROUP_ID = "flink-btc-enrichment-group";
}
