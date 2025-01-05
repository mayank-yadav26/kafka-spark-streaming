package mayank.yadav.kafka;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.stream.Stream;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

import kafka.utils.threadsafe;

public class CSVKafkaProducer {

    private static final String KafkaBrokerEndpoint = "localhost:9092";
    private static final String KafkaTopic = "demo";
    private static final String CsvFile = "/home/mayank/MyProjects/kafka-spark-streaming-integration/test.csv";

    private Producer<String, String> ProducerProperties() {
        Properties properties = new Properties();
        properties.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KafkaBrokerEndpoint);
        properties.put(ProducerConfig.CLIENT_ID_CONFIG, "KafkaCsvProducer");
        properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.put(ProducerConfig.ACKS_CONFIG, "all");
        properties.put(ProducerConfig.REQUEST_TIMEOUT_MS_CONFIG, 30000); // 30 seconds
        properties.put(ProducerConfig.MAX_BLOCK_MS_CONFIG, 60000); // 60 seconds

        return new KafkaProducer<String, String>(properties);
    }

    public static void main(String[] args) throws URISyntaxException {

        CSVKafkaProducer kafkaProducer = new CSVKafkaProducer();
        kafkaProducer.publishMessages();
        // try {
        //     Thread.sleep(1000L);
        // } catch (InterruptedException e) {
        //     // TODO Auto-generated catch block
        //     e.printStackTrace();
        // }
        System.out.println("Producing job completed");
    }

    private void publishMessagesTemp() throws URISyntaxException {

        final KafkaProducer<String, String> csvProducer = (KafkaProducer<String, String>) ProducerProperties();

        try {
            URI uri = getClass().getClassLoader().getResource(CsvFile).toURI();
            Stream<String> FileStream = Files.lines(Paths.get(uri));

            FileStream.forEach(line -> {
                System.out.println("line:"+line);

                final ProducerRecord<String, String> csvRecord = new ProducerRecord<String, String>(
                        KafkaTopic, UUID.randomUUID().toString(), line);

                csvProducer.send(csvRecord, (metadata, exception) -> {
                    if (metadata != null) {
                        System.out.println("CsvData: -> " + csvRecord.key() + " | " + csvRecord.value());
                    } else {
                        System.out.println("Error Sending Csv Record -> " + csvRecord.value()+ " | " + exception.getMessage());
                    }
                });
            });
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    
    private void publishMessages() throws URISyntaxException {
        final KafkaProducer<String, String> csvProducer = (KafkaProducer<String, String>) ProducerProperties();

        try (BufferedReader br = new BufferedReader(new FileReader(CsvFile))) {
            String line;
            while ((line = br.readLine()) != null) {
                System.out.println("line:"+line);
                ProducerRecord<String, String> record = new ProducerRecord<>(KafkaTopic, line);
                try {
                    csvProducer.send(record).get();
                } catch (InterruptedException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                } catch (ExecutionException e) {
                    // TODO Auto-generated catch block
                    e.printStackTrace();
                }
                System.out.println("Record sent to Kafka");
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            // csvProducer.flush();
            csvProducer.close();
        }
    }
}
