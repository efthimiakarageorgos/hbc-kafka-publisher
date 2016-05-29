package com.qiotec.qa.tools.kafkapublisher;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class QioKafkaPublisher {
  private KafkaProducer<String,String> producer;

  public QioKafkaPublisher(String bootstapServers) {
    Properties props = new Properties();
    props.put("bootstrap.servers", bootstapServers);
    props.put("serializer.class", "kafka.serializer.StringEncoder");
    producer= new KafkaProducer(props, new StringSerializer(), new StringSerializer());
  }

  public RecordMetadata publish(String topic, String key, String value) throws ExecutionException, InterruptedException {
    ProducerRecord<String,String> record = new ProducerRecord(topic, key, value);
    Future<RecordMetadata>	future = producer.send(record);
    return future.get();
  }

  public void close() {
    producer.close();
  }
}
