import java.util.Scanner;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

/**
 * Reads lines from stdin and sends each one to the Kafka topic.
 */
public class MyKafkaProducer {

    public static void main(String[] args) {
        System.out.println("Creating producer...");
        try (Producer<String, String> producer = new KafkaProducer<>(KafkaConfig.producerProperties());
             Scanner in = new Scanner(System.in)) {
            System.out.println("Producer created! Type messages, one per line.");
            while (in.hasNextLine()) {
                String msg = in.nextLine();
                producer.send(new ProducerRecord<>(KafkaConfig.topic(), msg));
                System.out.println("Sent:" + msg);
            }
        }
    }

}
