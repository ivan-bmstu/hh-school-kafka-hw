package ru.hh.kafkahw.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonSerializer;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;
import ru.hh.kafkahw.messages.Message;

import java.util.*;

@Component
public class Sender {
  private final KafkaProducer producer;
  private final ObjectMapper serializer;

  @Autowired
  public Sender(KafkaProducer producer) {
    this.producer = producer;
    serializer = new ObjectMapper();
  }

  public void doSomething(String topic, String message)  {
    Message msg = new Message(message, UUID.randomUUID());
    String serializedMsg;
    try {
      serializedMsg = serializer.writeValueAsString(msg);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
    send(topic, msg.getUuid().toString(), serializedMsg);
  }

  private void send(String topic, String key, String message){
    try {
      producer.send(topic, key, message);
    } catch (Exception e){
      send(topic, key, message);
    }
  }
}
