package service;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;
import io.prometheus.client.CollectorRegistry;
import io.prometheus.client.exporter.HTTPServer;
import io.prometheus.client.hotspot.DefaultExports;
import java.io.IOException;
import java.net.InetSocketAddress;

public class Server {

    HttpServer server;
    KafkaStreamLifecycle kafkaStream;

    public Server(KafkaStreamLifecycle kafkaStream) {
        this.kafkaStream = kafkaStream;
    }

    public Server start() throws IOException {
        server = HttpServer.create(new InetSocketAddress(Config.PORT), 0);
        up(server);
        ready(server);
        if (Config.USE_PROMETHEUS) {
            DefaultExports.initialize();
            new HTTPServer(server, CollectorRegistry.defaultRegistry, false);
        } else {
            server.start();
        }
        return this;
    }

    public void close() {
        server.stop(0);
    }

    private void up(HttpServer server) {
        var httpContext = server.createContext("/up");
        httpContext.setHandler(
            new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    if (!exchange.getRequestMethod().equals("GET")) {
                        exchange.sendResponseHeaders(404, -1);
                        return;
                    }
                    exchange.sendResponseHeaders(204, -1);
                }
            }
        );
    }

    private void ready(HttpServer server) {
        var httpContext = server.createContext("/ready");
        httpContext.setHandler(
            new HttpHandler() {
                @Override
                public void handle(HttpExchange exchange) throws IOException {
                    if (!exchange.getRequestMethod().equals("GET")) {
                        exchange.sendResponseHeaders(404, -1);
                        return;
                    }
                    if (!kafkaStream.ready()) {
                        Monitor.serviceNotReady(kafkaStream.getState());
                        exchange.sendResponseHeaders(500, -1);
                        return;
                    }
                    exchange.sendResponseHeaders(204, -1);
                }
            }
        );
    }
}
