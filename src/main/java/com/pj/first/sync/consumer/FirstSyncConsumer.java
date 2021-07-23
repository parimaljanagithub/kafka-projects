package com.pj.first.sync.consumer;

import com.pj.first.config.IFirstConfig;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Arrays;
import java.util.Properties;

/**
 * Copyright (c) 1997 - 2020 Rakuten, Inc. All Rights Reserved.
 * User: parimal.jana
 * Date: 23/07/21
 */
public class FirstSyncConsumer {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String args[]) throws UnknownHostException  {
      new FirstSyncConsumer().consumer();
    }

    private void consumer() throws UnknownHostException {
        Properties props = prepareConfig();
        KafkaConsumer<Integer, String> consumer = new KafkaConsumer<Integer, String>(props);
        //Kafka Consumer subscribes list of topics here.
        consumer.subscribe(Arrays.asList(IFirstConfig.topicName));
        while(true) {
            ConsumerRecords<Integer, String> records = consumer.poll(100);
            for(ConsumerRecord<Integer, String> r: records) {
                System.out.println(r.key() + "= " + r.value());
            }
        }

    }

    private Properties prepareConfig()  throws UnknownHostException {
        // create instance for properties to access producer configs
        Properties props = new Properties();
        props.put(ConsumerConfig.CLIENT_ID_CONFIG,  InetAddress.getLocalHost().getHostName());
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, IFirstConfig.bootstrapServers);
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class.getName());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        //props.put(ConsumerConfig.ACKS_CONFIG, "all");
        //If the request fails, the producer can automatically retry,
        //props.put(ConsumerConfig.RETRIES_CONFIG, 0);
        props.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "true");
        props.put(ConsumerConfig.AUTO_COMMIT_INTERVAL_MS_CONFIG, "1000");
        props.put(ConsumerConfig.SESSION_TIMEOUT_MS_CONFIG, "30000");
        props.put("group.id", "test");
        return props;


    }
}
