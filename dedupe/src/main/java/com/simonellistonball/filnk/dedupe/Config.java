package com.simonellistonball.filnk.dedupe;

import lombok.Getter;
import org.apache.flink.api.java.utils.ParameterTool;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

@Getter
public class Config {

    public static final String K_SCHEMA_REG_URL = "schema.registry.url";
    public static final String K_SCHEMA_REG_SSL_CLIENT_KEY = "schema.registry.client.ssl";
    public static final String K_TRUSTSTORE_PATH = "trustStorePath";
    public static final String K_TRUSTSTORE_PASSWORD = "trustStorePassword";
    public static final String K_KEYSTORE_PASSWORD = "keyStorePassword";
    private static final String INPUT_TOPIC_KEY = "inputTopic";
    private static final String OUTPUT_TOPIC_KEY = "outputTopic";
    private static final String BOOTSTRAP_SERVERS_KEY = "bootstrapServers";
    private static final String OFFSET_KEY = "offset";
    private static final String PARALLELISM_KEY = "parallelism";
    private static final String SLEEPTIME_KEY = "sleep";
    private static final String INPUT_FILE_KEY = "inputFile";

    private final String inputTopic;
    private final String outputTopic;

    private final String bootstrapServers;
    private final String offset;

    private final Integer parallelism;

    private final int sleepTime;
    private final File inputFile;


    private ParameterTool params;

    public Config(ParameterTool params) {
        this.params = params;
        this.inputTopic = this.params.get(INPUT_TOPIC_KEY);
        this.outputTopic = this.params.get(OUTPUT_TOPIC_KEY);
        this.bootstrapServers = this.params.get(BOOTSTRAP_SERVERS_KEY);
        this.offset = this.params.get(OFFSET_KEY, "latest");
        this.parallelism = this.params.getInt(PARALLELISM_KEY, 1);
        this.sleepTime = this.params.getInt(SLEEPTIME_KEY, 10);
        this.inputFile = new File(this.params.get(INPUT_FILE_KEY));
    }

    public Map<String, ?> getSchemaRegistryProperties() {

        Map<String, String> sslClientConfig = new HashMap<>();
        sslClientConfig.put(K_TRUSTSTORE_PATH, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PATH));
        sslClientConfig.put(K_TRUSTSTORE_PASSWORD, params.get(K_SCHEMA_REG_SSL_CLIENT_KEY + "." + K_TRUSTSTORE_PASSWORD));

        Map<String, Object> schemaRegistryConf = new HashMap<>();
        schemaRegistryConf.put(K_SCHEMA_REG_URL, params.get(K_SCHEMA_REG_URL));
        schemaRegistryConf.put(K_SCHEMA_REG_SSL_CLIENT_KEY, sslClientConfig);

        return schemaRegistryConf;
    }

}
