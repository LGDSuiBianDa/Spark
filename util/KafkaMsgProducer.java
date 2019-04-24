package com.yd.spark.util;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.io.IOException;
import java.util.Properties;

/**
  * Created by suibianda LGD  on 2019/4/17 17:38
  * Modified by: 
  * Version: 0.0.1
  * Usage: kafka message producer工具类
  *
  */
public class KafkaMsgProducer {

    // Declare a new producer
    public static KafkaProducer producer;

    public static void main(String[] args) throws IOException,InterruptedException {

        // Set the default stream and topic to publish to.
        String topic = PropertiesLoader.getInstance().getProperty("topic.name");
        String fileName = PropertiesLoader.getInstance().getProperty("file.name");
        //String topic = "xxxx";
        //String fileName = "/x/x/x/x.json";

        configureProducer();
        File f = new File(fileName);
        FileReader fr = new FileReader(f);
        BufferedReader reader = new BufferedReader(fr);
        String line = reader.readLine();
        while (line != null) {
  
            /* Add each message to a record. A ProducerRecord object
             identifies the topic or specific partition to publish
             a message to. */
            ProducerRecord<String, String> rec = new ProducerRecord<>(topic,  line);

            // Send the record to the producer client library.
            producer.send(rec);
            Thread.sleep(5000);
            System.out.println("Sent message: " + line);
            line = reader.readLine();

        }

        producer.close();
        System.out.println("All done.");

        System.exit(1);

    }

    /* Set the value for a configuration parameter.
     This configuration parameter specifies which class
     to use to serialize the value of each message.*/
    public static void configureProducer() {
        String brokers = PropertiesLoader.getInstance().getProperty("kafka.brokers");
        Properties props = new Properties();
        props.put("bootstrap.servers",brokers);
        props.put("key.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer",
                "org.apache.kafka.common.serialization.StringSerializer");

        producer = new KafkaProducer<>(props);
    }

}
