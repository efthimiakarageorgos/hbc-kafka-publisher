/**
 * Copyright (C) 2016 QIO Techonologies (admi@qiotec.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hbc.qa.tools.kafkapublisher;


import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;


public class HbcKafkaPublisher {
  private KafkaProducer<String,String> producer;

  public HbcKafkaPublisher(String bootstapServers) {
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
