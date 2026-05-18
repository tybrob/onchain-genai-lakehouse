package com.onchain.config;

public class Configuration {
    public static final String WS_URL = "wss://ws.blockchain.info/inv";
    public static final String OP_SUBSCRIBE_UNCONFIRMED = "{\"op\":\"unconfirmed_sub\"}";
    public static final String OP_RESPONSE_UTX = "utx";
    public static final String KAFKA_BOOTSTRAP_SERVERS = System.getenv("KAFKA_BOOTSTRAP_SERVERS");
    public static final String KAFKA_TOPIC = "btc-mempool";
    public static final String KAFKA_CLIENT_ID = "btc-listener-raw-producer";
}