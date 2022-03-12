package service;

import io.prometheus.client.Counter;
import java.util.ArrayList;

import org.apache.kafka.streams.KafkaStreams.State;
import org.json.JSONObject;

public class Monitor {

    private static Counter incoming = Counter.build().labelNames("topic").name("incoming").help("incoming").register();
    private static Counter outgoing = Counter.build().labelNames("topic").name("outgoing").help("outgoing").register();

    public static void serviceStarted() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", Config.APPLICATION_ID_CONFIG + " started");

        write(log);
    }

    public static void serviceNotReady(State state) {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", Config.APPLICATION_ID_CONFIG + " not ready")
            .put("stream state", state);

        write(log);
    }

    public static void incoming(String topic, String value) {
        incoming.labels(topic).inc();
        write(
            new JSONObject()
                .put("level", "info")
                .put("message", "incoming " + topic + " " + System.currentTimeMillis())
                .put("extra", new JSONObject().put("value", value))
        );
    }

    public static void outgoing(String topic, String value) {
        outgoing.labels(topic).inc();
        write(
            new JSONObject()
                .put("level", "info")
                .put("message", "outgoing " + topic + " " + System.currentTimeMillis())
                .put("extra", new JSONObject().put("value", value))
        );
    }

    public static void initializationError(Throwable exception) {
        JSONObject log = new JSONObject()
            .put("level", "error")
            .put("message", "Unexpected error while initializing")
            .put(
                "err",
                new JSONObject().put("errorMessages", getErrorMessages(exception)).put("class", exception.getClass())
            );

        write(log);
    }

    public static void serviceShutdown() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-stream-" + Config.APPLICATION_ID_CONFIG + " shutting down");

        write(log);
    }

    public static void serviceTerminated() {
        JSONObject log = new JSONObject()
            .put("level", "info")
            .put("message", "kafka-stream-" + Config.APPLICATION_ID_CONFIG + " terminated");

        write(log);
    }

    private static void write(JSONObject log) {
        System.out.println(log.toString());
    }

    private static ArrayList<String> getErrorMessagesArray(Throwable exception, ArrayList<String> messages) {
        if (exception == null) {
            return messages;
        }
        messages.add(exception.getMessage());
        return getErrorMessagesArray(exception.getCause(), messages);
    }

    private static JSONObject getErrorMessages(Throwable exception) {
        var messages = getErrorMessagesArray(exception, new ArrayList<String>());
        var errorMessages = new JSONObject();
        for (var i = 0; i < messages.size(); i++) {
            errorMessages.put("message" + i, messages.get(i));
        }
        return errorMessages;
    }
}
