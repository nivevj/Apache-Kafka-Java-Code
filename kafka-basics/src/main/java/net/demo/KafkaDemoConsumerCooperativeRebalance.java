package net.demo;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

//TIP To <b>Run</b> code, press <shortcut actionId="Run"/> or
// click the <icon src="AllIcons.Actions.Execute"/> icon in the gutter.
public class KafkaDemoConsumerCooperativeRebalance {

    private static Logger log = LoggerFactory.getLogger(KafkaDemoConsumerCooperativeRebalance.class.getSimpleName());

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
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());
        //properties.setProperty("group.instance.id","...") for static assignment

        //create consumer
        KafkaConsumer<String, String> kafkaConsumer=new KafkaConsumer<>(properties);

        // get reference to main Thread
        final Thread mainThread = Thread.currentThread();

        //adding shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(){
            public void run() {
                log.info("Detected shutdown, exit by calling consumer.wakeup()");
                kafkaConsumer.wakeup();

                //join the main thread to allow execution of the code in main thread
                try {
                    mainThread.join();
                } catch (InterruptedException e) {
                    throw new RuntimeException(e);
                }
            }
        });

        //subscribe to a topic
        kafkaConsumer.subscribe(Arrays.asList(topic));

        //poll for data
        try {
            while (true) {
                ConsumerRecords<String, String> records = kafkaConsumer.poll(Duration.ofMillis(1000));
                for (ConsumerRecord<String, String> record : records) {
                    log.info("Key: " + record.key() + " Value: " + record.value());
                    log.info("Partition: " + record.partition() + " Offset: " + record.offset());
                }
            }
        }
        catch (WakeupException e){
            log.info("Consumer is starting to shutdown");
        }
        catch (Exception e){
            log.info("Unexpected exception ",e);
        }
        finally {
            //close consumer and commit offsets
            kafkaConsumer.close();
            log.info("Consumer is shutdown");
        }

    }
}