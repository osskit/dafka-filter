package service;

public class Main {

    static Config config;
    static KafkaStreamLifecycle kafkaStream;
    static Server server;

    public static void main(String[] args) throws Exception {
        try {
            Config.init();
            Monitor.serviceStarted();

            kafkaStream = new KafkaStreamLifecycle(new Stream());
            kafkaStream.start();
            server = new Server(kafkaStream).start();

            Runtime
                .getRuntime()
                .addShutdownHook(
                    new Thread(() -> {
                        Monitor.serviceShutdown();
                        server.close();
                        gracefully();
                        kafkaStream.close();
                        Monitor.serviceTerminated();
                    })
                );
        } catch (Exception e) {
            Monitor.initializationError(e);
        }
    }

    private static void gracefully() {
        try {
            Thread.sleep(10000);
        } catch (InterruptedException e) {}
    }
}
