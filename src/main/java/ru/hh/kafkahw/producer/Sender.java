package ru.hh.kafkahw.producer;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.KafkaProducer;

import java.util.HashSet;
import java.util.Set;

@Component
public class Sender {
  private final KafkaProducer producer;
  private final Set<String> importantTopics;

  @Autowired
  public Sender(KafkaProducer producer) {
    this.producer = producer;
    importantTopics = new HashSet<>();
    importantTopics.add("topic2");
    importantTopics.add("topic3");
  }

  public void doSomething(String topic, String message)  {
    try {
      producer.send(topic, message);
    } catch (Exception e) {
      if (isNeedRetry(topic)){
        doSomething(topic, message);
      }
    }
  }

  private boolean isNeedRetry(String topicName){
    return importantTopics.contains(topicName);
  }
}
