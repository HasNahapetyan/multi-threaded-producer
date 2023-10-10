package guru.learningjournal.kafka.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;

@Slf4j
public class Dispatcher implements Runnable {
    private String fileLocation;
    private String topicName;
    private KafkaProducer<Integer, String> producer;


    public Dispatcher(String fileLocation, String topicName, KafkaProducer<Integer, String> producer) {
        this.fileLocation = fileLocation;
        this.topicName = topicName;
        this.producer = producer;
    }

    public void run() {
        log.info("Start processing " + fileLocation);
        File file = new File(fileLocation);
        int counter = 0;
        try (Scanner scanner = new Scanner(file)) {
            while (scanner.hasNextLine()) {
                String line = scanner.nextLine();
                producer.send(new ProducerRecord<>(topicName, null, line));
                counter++;
            }
            log.info("Finished sending " + counter + " messages from " + fileLocation);
        } catch (FileNotFoundException e) {
            throw new RuntimeException(e);
        }

    }
}
