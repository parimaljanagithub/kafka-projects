package com.pj.first.sync.producer;

import com.pj.first.config.IFirstConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.IntegerSerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.Properties;

/**
 * Copyright (c) 1997 - 2020 Rakuten, Inc. All Rights Reserved.
 * User: parimal.jana
 * Date: 23/07/21
 */
public class FirstSyncProducer {
    private static final Logger logger = LogManager.getLogger();
    public static void main(String args[]) throws UnknownHostException  {
        FirstSyncProducer fp = new FirstSyncProducer();
        fp.producer();
    }

    private void producer() throws UnknownHostException {
        Properties props = prepareConfig();
        Producer<Integer, String> producer = new KafkaProducer<Integer, String>(props);

        for(int i = 0; i < 10; i++){
            producer.send(new ProducerRecord<Integer, String>(IFirstConfig.topicName,
                    i, "Hello-".concat(Integer.toString(i))));
        }
        producer.close();
        logger.info("Finished - Closing Kafka Producer.");
        System.out.println("Successfully send the message to kafka");
    }


    private Properties prepareConfig()  throws UnknownHostException {
        // create instance for properties to access producer configs
        Properties props = new Properties();
        props.put(ProducerConfig.CLIENT_ID_CONFIG,  InetAddress.getLocalHost().getHostName());
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, IFirstConfig.bootstrapServers);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, IntegerSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        //If the request fails, the producer can automatically retry,
        props.put(ProducerConfig.RETRIES_CONFIG, 0);
        return props;


    }
}
