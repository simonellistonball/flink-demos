package com.simonellistonball.flink.dedupe;

import lombok.extern.slf4j.Slf4j;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.util.Preconditions;
import org.apache.flink.util.encrypttool.EncryptTool;

import java.io.IOException;
import java.util.Properties;

@Slf4j
public class Utils {
    public static final String K_PROPERTIES_FILE = "properties.file";
    public static final String SENSITIVE_KEYS_KEY = "sensitive.keys";

    public static final String KAFKA_PREFIX = "kafka.";

    public static ParameterTool parseArgs(String[] args) throws IOException {

        // Processing job properties
        ParameterTool params = ParameterTool.fromArgs(args);
        if (params.has(K_PROPERTIES_FILE)) {
            params = ParameterTool.fromPropertiesFile(params.getRequired(K_PROPERTIES_FILE)).mergeWith(params);
        }

        log.info("### Job parameters:");
        for (String key : params.getProperties().stringPropertyNames()) {
            log.info("Job Param: {}={}", key, isSensitive(key, params) ? MASK : params.get(key));
        }
        return params;
    }
    public static Properties readKafkaProperties(ParameterTool params, boolean consumer) {
        Properties properties = new Properties();
        for (String key : params.getProperties().stringPropertyNames()) {
            if (key.startsWith(KAFKA_PREFIX)) {
                properties.setProperty(key.substring(KAFKA_PREFIX.length()), params.get(key));
            }
        }

        log.info("### Kafka parameters:");
        for (String key : properties.stringPropertyNames()) {
            log.info("Kafka param: {}={}", key, properties.get(key));
        }
        return properties;
    }

    public static boolean isSensitive(String key, ParameterTool params) {
        Preconditions.checkNotNull(key, "key is null");
        final String value = params.get(SENSITIVE_KEYS_KEY);
        if (value == null) return false;
        String keyInLower = key.toLowerCase();
        String[] sensitiveKeys = value.split(",");

        for (int i = 0; i < sensitiveKeys.length; ++i) {
            String hideKey = sensitiveKeys[i];
            if (keyInLower.length() >= hideKey.length() && keyInLower.contains(hideKey)) {
                return true;
            }
        }
        return false;
    }

    public static String decrypt(String input) {
        Preconditions.checkNotNull(input, "key is null");
        return EncryptTool.getInstance().decrypt(input);
    }

}
