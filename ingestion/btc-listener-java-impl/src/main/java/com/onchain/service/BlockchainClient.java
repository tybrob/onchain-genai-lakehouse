package com.onchain.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.WebSocket;
import java.util.concurrent.CompletionStage;

public class BlockchainClient {
    private static final Logger logger = LoggerFactory.getLogger(BlockchainClient.class);
    private final String url;
    private final KafkaProducerService kafkaService;

    public BlockchainClient(String url, KafkaProducerService kafkaService) {
        this.url = url;
        this.kafkaService = kafkaService;
    }

    public void start(String subscriptionMessage) {
        HttpClient client = HttpClient.newHttpClient();
        logger.info("Connecting to WebSocket: {}", url);

        client.newWebSocketBuilder()
                .buildAsync(URI.create(url), new WebSocketListener(subscriptionMessage))
                .join();

        while (true) {
            try {
                Thread.sleep(10000);
            } catch (InterruptedException e) {
                break;
            }
        }
    }

    private class WebSocketListener implements WebSocket.Listener {
        private final String subscriptionMsg;

        public WebSocketListener(String msg) {
            this.subscriptionMsg = msg;
        }

        @Override
        public void onOpen(WebSocket webSocket) {
            webSocket.sendText(subscriptionMsg, true);
            WebSocket.Listener.super.onOpen(webSocket);
        }

        @Override
        public CompletionStage<?> onText(WebSocket webSocket, CharSequence data, boolean last) {
            kafkaService.process(data.toString());
            webSocket.request(1);
            return null;
        }
    }
}
