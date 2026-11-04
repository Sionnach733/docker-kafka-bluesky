import java.util.Properties;

/**
 * Shared Kafka settings, read from environment variables so the same jar works
 * inside docker-compose (kafka:9092) and from the host (localhost:9094).
 */
final class KafkaConfig {

    private KafkaConfig() {
    }

    static String bootstrapServers() {
        return env("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094");
    }

    static String topic() {
        return env("KAFKA_TOPIC", "HelloKafkaTopic");
    }

    static Properties producerProperties() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers());
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        return props;
    }

    static String env(String name, String defaultValue) {
        String value = System.getenv(name);
        return (value == null || value.isEmpty()) ? defaultValue : value;
    }

    static String requireEnv(String name) {
        String value = System.getenv(name);
        if (value == null || value.isEmpty()) {
            throw new IllegalStateException("Missing required environment variable: " + name);
        }
        return value;
    }
}
