package service;

import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.KafkaStreams.State;
import org.apache.kafka.streams.StreamsBuilder;

public class KafkaStreamLifecycle {

    private KafkaStreams kafkaStreamsClient;
    private Stream stream;

    public KafkaStreamLifecycle(Stream stream) {
        this.stream = stream;
    }

    public void start() {
        StreamsBuilder builder = new StreamsBuilder();
        stream.create(builder);
        kafkaStreamsClient = KafkaStreamClientCreator.createStreamsClient(builder);
        kafkaStreamsClient.start();
    }

    public boolean ready() {
        return kafkaStreamsClient.state() == State.RUNNING;
    }

    public void close() {
        kafkaStreamsClient.close();
    }

    public State getState() {
        return kafkaStreamsClient.state();
    }
}
