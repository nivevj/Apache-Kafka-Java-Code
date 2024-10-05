package net.demo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class KafkaDemoConsumer {

    private static Logger log = LoggerFactory.getLogger(KafkaDemoConsumer.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Kafka consumer");

        String groupId= "my-java-application";
        String topic = "demo_topic_java";

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","[::1]:9092");
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());
        properties.setProperty("group.id",groupId);
        properties.setProperty("auto.offset.rest","earliest");

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer=new KafkaConsumer<>(properties);

        //subscribe to a topic
        kafkaConsumer.subscribe(Arrays.asList(topic));

        //poll for data
        while(true){
            log.info("Polling");
            ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
            for(ConsumerRecord<String,String> record: records){
                log.info("Key: "+record.key()+" Value: "+record.value());
                log.info("Partition: "+record.partition()+" Offset: "+record.offset());
            }
        }

    }
}