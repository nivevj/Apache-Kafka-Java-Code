package net.demo;
import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class KafkaDemoProducerCallBack {

    private static Logger log = LoggerFactory.getLogger(KafkaDemoProducerCallBack.class.getSimpleName());

    public static void main(String[] args) {

        log.info("Kafka producer with CallBack");

        //create producer properties
        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers","[::1]:9092");
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());
        //properties.setProperty("batch.size", "400");
        //properties.setProperty("partitioner.class", RoundRobinPartitioner.class.getName());

        //create producer
        KafkaProducer<String,String> kafkaProducer=new KafkaProducer<>(properties);

        for(int j=0;j<10;j++)
        {
            for (int i = 0; i < 30; i++) {
                //create producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_topic_java", "abc" + i);

                //send data
                kafkaProducer.send(producerRecord, new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata recordMetadata, Exception e) {
                        //executes everytime when a record is sent
                        if (e == null) {
                            //if sent successfully
                            log.info("Received new message\n" + "Topic: " + recordMetadata.topic() + "\nPartition: " + recordMetadata.partition() + "\nOffset: " + recordMetadata.offset() + "\nTimestamp: " + recordMetadata.timestamp());
                        } else {
                            log.info("Error in sending message\n", e);
                        }
                    }
                });
            }

            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        //tell producer to send all data
        kafkaProducer.flush();

        //flush and close the producer
        kafkaProducer.close();

    }
}