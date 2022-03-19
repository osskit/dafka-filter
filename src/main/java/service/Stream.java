package service;

import java.util.Arrays;
import com.fasterxml.jackson.databind.JsonNode;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Predicate;
import org.apache.kafka.streams.kstream.Produced;

public class Stream {
    public StreamsBuilder create(StreamsBuilder builder) {
        var jsonDeserializer = new JsonDeserializer();
        var jsonSerializer = new JsonSerializer();
        var jsonSerde = Serdes.serdeFrom(jsonSerializer, jsonDeserializer);

        builder
            .stream(Config.SOURCE_TOPIC, Consumed.with(Serdes.String(), jsonSerde))
            .peek((__, value) -> Monitor.incoming(Config.SOURCE_TOPIC, value.toPrettyString()))
            .filter(match())
            .peek((key, value) -> Monitor.outgoing(Config.DESTINATION_TOPIC, value.toPrettyString()))
            .to(Config.DESTINATION_TOPIC, Produced.with(Serdes.String(), jsonSerde));

        return builder;
    }

    private Predicate<String, JsonNode> match() {
        return new Predicate<String, JsonNode>() {
            @Override
            public boolean test(String key, JsonNode value) {
                return Arrays.asList(Config.FILTERS.split(",")).stream()
                        .map(s -> s.split(":"))
                        .map(s -> new Filter(s[0], s[1].substring(0, 2), s[1].substring(2)))
                        .map(filter -> {
                            if (filter.operator == Operator.Matches) {
                                return value.at(filter.path).asText().matches(filter.value);
                            } else if (filter.operator == Operator.NotMatches) {
                                return !value.at(filter.path).asText().matches(filter.value);
                            } else {
                                return false;
                            }
                        })
                        .allMatch(x -> x == true);
            }
        };
    }
}

class Filter {
    public String path;
    public String value;
    public Operator operator;

    public Filter(String path, String operator, String value) {
        this.path = path;
        if (operator.equals("==")) {
            this.operator = Operator.Matches;
        } else if (operator.equals("!=")) {
            this.operator = Operator.NotMatches;
        }
        this.value = value;
    }
}

enum Operator {
    Matches,
    NotMatches,
}
