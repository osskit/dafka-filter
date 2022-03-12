package service;

import io.github.cdimascio.dotenv.Dotenv;
import java.nio.file.Files;
import java.nio.file.Paths;

public class Config {

    public static int PORT;
    public static String APPLICATION_ID_CONFIG;
    public static String KAFKA_BROKER;
    public static boolean USE_SASL_AUTH;
    public static String SASL_USERNAME;
    public static String SASL_PASSWORD;
    public static String TRUSTSTORE_FILE_PATH;
    public static String TRUSTSTORE_PASSWORD;
    public static boolean USE_PROMETHEUS;
    public static String SOURCE_TOPIC;
    public static String FILTERS;
    public static String DESTINATION_TOPIC;

    public static void init() throws Exception {
        Dotenv dotenv = Dotenv.configure().ignoreIfMissing().load();

        PORT = getInt(dotenv, "PORT");
        KAFKA_BROKER = getString(dotenv, "KAFKA_BROKER");
        APPLICATION_ID_CONFIG = getString(dotenv, "APPLICATION_ID_CONFIG");

        SOURCE_TOPIC = getString(dotenv, "SOURCE_TOPIC");
        FILTERS = getString(dotenv, "FILTERS");
        DESTINATION_TOPIC = getString(dotenv, "DESTINATION_TOPIC");

        USE_SASL_AUTH = getOptionalBool(dotenv, "USE_SASL_AUTH", false);
        if (USE_SASL_AUTH) {
            SASL_USERNAME = getString(dotenv, "SASL_USERNAME");
            SASL_PASSWORD = getStringValueOrFromFile(dotenv, "SASL_PASSWORD");
            TRUSTSTORE_FILE_PATH = getOptionalString(dotenv, "TRUSTSTORE_FILE_PATH", null);
            if (TRUSTSTORE_FILE_PATH != null) {
                TRUSTSTORE_PASSWORD = getStringValueOrFromFile(dotenv, "TRUSTSTORE_PASSWORD");
            }
        }

        USE_PROMETHEUS = getOptionalBool(dotenv, "USE_PROMETHEUS", false);
    }

    private static int getInt(Dotenv dotenv, String name) {
        return Integer.parseInt(dotenv.get(name));
    }

    private static String getString(Dotenv dotenv, String name) throws Exception {
        String value = dotenv.get(name);

        if (value == null) {
            throw new Exception("missing env var: " + name);
        }

        return value;
    }

    private static String getStringValueOrFromFile(Dotenv dotenv, String name) throws Exception {
        String value = dotenv.get(name);

        if (value != null) {
            return value;
        }

        String filePath = dotenv.get(name + "_FILE_PATH");

        if (filePath == null) {
            throw new Exception("missing env var: " + name + " or " + name + "_FILE_PATH");
        }

        return new String(Files.readAllBytes(Paths.get(filePath)));
    }

    private static String getOptionalString(Dotenv dotenv, String name, String fallback) {
        try {
            return getString(dotenv, name);
        } catch (Exception e) {
            return fallback;
        }
    }

    private static boolean getOptionalBool(Dotenv dotenv, String name, boolean fallback) {
        try {
            return Boolean.parseBoolean(getString(dotenv, name));
        } catch (Exception e) {
            return fallback;
        }
    }
}
