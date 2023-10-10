package guru.learningjournal.kafka.examples;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

@Slf4j
public class DispatcherDemo {

    public static void main(String[] args) {
        log.info("starting Dispatcher demo.................");
        Properties properties = new Properties();
        try {
            InputStream inputStream = Files.newInputStream(Paths.get(AppConfigs.kafkaConfigFileLocation));
            properties.load(inputStream);
            properties.put(ProducerConfig.CLIENT_ID_CONFIG, AppConfigs.applicationID);
            properties.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
            properties.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        KafkaProducer<Integer, String> producer = new KafkaProducer<>(properties);
        int length = AppConfigs.eventFiles.length;
        Thread[] dispatchers = new Thread[length];
        log.info("starting Dispatchers");
        for (int i = 0; i < length; i++) {
            //All the threads are using the same producer
            dispatchers[i] = new Thread(new Dispatcher(AppConfigs.eventFiles[i], AppConfigs.topicName, producer));
            dispatchers[i].start();
        }
        try {
            for (Thread t : dispatchers) t.join();
        } catch (InterruptedException e) {
            log.error("Main Thread Interrupted");
            throw new RuntimeException(e);
        } finally {
            producer.close();
            log.info("Finished Dispatcher Demo");
        }

    }
}
