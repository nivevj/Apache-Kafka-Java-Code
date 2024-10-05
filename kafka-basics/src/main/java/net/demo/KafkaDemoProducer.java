package net.demo;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class KafkaDemoProducer {

    private static Logger log = LoggerFactory.getLogger(KafkaDemoProducer.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Kafka producer");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","[::1]:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        //create producer
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<>(properties);

        //create producer record
        ProducerRecord<String,String> producerRecord=new ProducerRecord<>("demo_topic_java","newmsg1");

        //send data
        kafkaProducer.send(producerRecord);

        //tell producer to send all data
        kafkaProducer.flush();

        //flush and close the producer
        kafkaProducer.close();

    }
}