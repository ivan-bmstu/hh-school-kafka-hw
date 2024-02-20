package ru.hh.kafkahw.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;
import ru.hh.kafkahw.messages.Message;


@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;
  private final ObjectMapper deserializer;
  private final SetUUIDMessages wroteId;

  public TopicListener(Service service) {
    this.service = service;
    this.deserializer = new ObjectMapper();
    this.wroteId = new SetUUIDMessages();
  }

  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    ack.acknowledge();
    String topic = consumerRecord.topic();
    String data =  consumerRecord.value();
    LOGGER.info("Try handle message, topic {}, payload {}", topic, data);
    Message msg = getMessage(data);
    if(wroteId.isNew(msg.getUuid())){
      wroteId.writeId(msg.getUuid());
      try {
        service.handle(topic, msg.getContent());
      } catch (Exception e){
        LOGGER.error("FAILED AT MESSAGE '{}' ERROR OCCUR AT TOPIC: {}", consumerRecord.value(), consumerRecord.topic());
      }
    }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    String topic = consumerRecord.topic();
    String data =  consumerRecord.value();
    LOGGER.info("Try handle message, topic {}, payload {}", topic, data);
    Message msg = getMessage(data);
    try {
      service.handle(topic, msg.getContent());
    } catch (Exception e){
      LOGGER.error("FAILED AT MESSAGE '{}' ERROR OCCUR AT TOPIC: {}", consumerRecord.value(), consumerRecord.topic());
      throw  new RuntimeException();
    }
    ack.acknowledge();
  }

  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    String topic = consumerRecord.topic();
    String data =  consumerRecord.value();
    LOGGER.info("Try handle message, topic {}, payload {}", topic, data);
    Message msg = getMessage(data);
    if(wroteId.isNew(msg.getUuid()) && (service.count(topic, msg.getContent()) < 1)){
      try {
        service.handle(topic, msg.getContent());
      } catch (Exception e){
        LOGGER.error("FAILED AT MESSAGE '{}' ERROR OCCUR AT TOPIC: {}", consumerRecord.value(), consumerRecord.topic());
        throw  new RuntimeException();
      }
    }
    wroteId.writeId(msg.getUuid());
    ack.acknowledge();
  }

   Message getMessage(String json){
     try {
       return deserializer.readValue(json, Message.class);
     } catch (JsonProcessingException e) {
       throw new RuntimeException(e);
     }
   }

}
