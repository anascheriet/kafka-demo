package com.acheriet.kafka.demo;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithKeys {
    public static void main(String[] args) {

        Logger logger = LoggerFactory.getLogger(ProducerDemoWithCallback.class);

        // create producer properties
        String bootstrapServers = "127.0.0.1:9092";
        Properties properties = new Properties();
        properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
        properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // create producer
        KafkaProducer<String, String> producer = new KafkaProducer<String, String>(properties);

        for (int i = 0; i < 10; i++) {
            String topic = "first_topic";
            String value = "hello world " + Integer.toString(i);
            String key = "id_" + Integer.toString(i);

            // create the data to send (producer record)
            ProducerRecord<String, String> record = new ProducerRecord<String, String>(topic, key, value);
            // send data
            producer.send(record, new Callback() {
                @Override
                public void onCompletion(RecordMetadata metadata, Exception e) {
                    // executes everytime a record is successfully sent or an exception is thrown
                    if (e == null) {
                        logger
                                .info(
                                        "Received ne metadata. \n" + "Topic: " + metadata.topic() + "\n" + "Partition "
                                                + metadata.partition() + "\n" + "Offset: " + metadata.offset() + "\n"
                                                + "TimeStamp: " + metadata.timestamp() + "\n" + "Key: " + key);
                    }
                    else {
                        logger.error("Error while producing", e);
                    }
                }
            });
        }
        producer.flush();
    }
}
