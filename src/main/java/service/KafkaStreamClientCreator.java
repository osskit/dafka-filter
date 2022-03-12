package service;

import java.util.Properties;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;

class KafkaStreamClientCreator {

    private KafkaStreamClientCreator() {}

    private static Properties getAuthProperties() {
        var props = new Properties();

        if (!Config.USE_SASL_AUTH) {
            return props;
        }

        props.put("security.protocol", "SASL_SSL");

        if (Config.TRUSTSTORE_PASSWORD != null) {
            props.put("ssl.truststore.location", Config.TRUSTSTORE_FILE_PATH);
            props.put("ssl.truststore.password", Config.TRUSTSTORE_PASSWORD);
        }

        props.put("sasl.mechanism", "PLAIN");
        props.put("ssl.endpoint.identification.algorithm", "");
        props.put(
            "sasl.jaas.config",
            String.format(
                "org.apache.kafka.common.security.plain.PlainLoginModule required username=\"%s\" password=\"%s\";",
                Config.SASL_USERNAME,
                Config.SASL_PASSWORD
            )
        );

        return props;
    }

    public static KafkaStreams createStreamsClient(StreamsBuilder builder) {
        Topology topology = builder.build();
        Properties props = getAuthProperties();

        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, Config.KAFKA_BROKER);
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, Config.APPLICATION_ID_CONFIG);

        return new KafkaStreams(topology, props);
    }
}
