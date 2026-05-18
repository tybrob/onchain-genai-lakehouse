package com.onchain.flink.mapper;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.apache.flink.api.common.functions.MapFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class EnrichmentMapper implements MapFunction<String, String> {

    private static final Logger logger = LoggerFactory.getLogger(EnrichmentMapper.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    @Override
    public String map(String value) {
        if (value == null || value.isBlank()) {
            logger.debug("Received null/blank message; passing through unchanged.");
            return value;
        }

        try {
            JsonNode root = mapper.readTree(value);

            if (!(root instanceof ObjectNode)) {
                logger.warn("Root JSON is not an object; passing through unchanged.");
                return value;
            }

            ObjectNode rootObj = (ObjectNode) root;

            JsonNode x = rootObj.get("x");
            if (x == null || !x.isObject()) {
                logger.debug("Message missing 'x' object; passing through unchanged.");
                return value;
            }

            JsonNode hashNode = x.get("hash");
            JsonNode timeNode = x.get("time");
            JsonNode inputsNode = x.get("inputs");
            JsonNode outputsNode = x.get("out");

            int totalInputs = (inputsNode != null && inputsNode.isArray()) ? inputsNode.size() : 0;
            int totalOutputs = (outputsNode != null && outputsNode.isArray()) ? outputsNode.size() : 0;

            if (hashNode != null && hashNode.isTextual()) {
                rootObj.put("tx_hash", hashNode.asText());
            } else {
                rootObj.putNull("tx_hash");
            }

            if (timeNode != null && timeNode.canConvertToLong()) {
                rootObj.put("tx_time", timeNode.asLong());
            } else {
                rootObj.putNull("tx_time");
            }

            rootObj.put("total_inputs", totalInputs);
            rootObj.put("total_outputs", totalOutputs);

            return mapper.writeValueAsString(rootObj);

        } catch (Exception e) {
            logger.warn("Failed to parse/enrich message; passing through unchanged. Error={}", e.toString());
            logger.debug("Offending payload: {}", value);
            return value;
        }
    }
}
