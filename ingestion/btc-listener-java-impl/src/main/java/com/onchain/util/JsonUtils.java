package com.onchain.util;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import java.util.Optional;

public class JsonUtils {
    private static final Logger logger = LoggerFactory.getLogger(JsonUtils.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    public static Optional<JsonNode> parse(String json) {
        try {
            return Optional.of(mapper.readTree(json));
        } catch (Exception e) {
            logger.error("Failed to parse JSON: {}", e.getMessage());
            return Optional.empty();
        }
    }
}
