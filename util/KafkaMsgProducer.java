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
 * 本地安装Kafka并用spark实时计算来消费，一般为以下几个步骤：
 * 1.启动zookeeper、Kafka
 * brew services start zookeeper
 * brew services start kafka
 * 首次需要先创建topic
 * kafka-topics --create --zookeeper localhost:2181 --replication-factor 1 --partitions 2 --topic ITMS_CHILD_PARENT_SCHEDULE
 * 查看创建的topic
 * kafka-topics --list --zookeeper localhost:2181
 *
 * 2.KafkaMsgProducer发送测试数据到topic
 * 也可以在终端上用下面的命令发送单条数据到指定Kafka
 * kafka-console-producer --broker-list localhost:9092 --topic ITMS_CHILD_PARENT_SCHEDULE
 *
 * 3.实时任务消费Kafka数据，验证计算逻辑
 *
 * 4.关闭zookeeper、Kafka
 * brew services stop zookeeper
 * brew services stop kafka
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
            ProducerRecord<String, String> rec = new ProducerRecord<String,String>(topic, line);

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

        producer = new KafkaProducer(props);
    }

}
