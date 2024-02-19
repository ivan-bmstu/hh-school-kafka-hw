package ru.hh.kafkahw.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  //не пытаемся считать повторно сообщения в случае неудачи
  @KafkaListener(topics = "topic1", groupId = "group1", errorHandler = "mostOnceSemantic")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    listen(consumerRecord, ack);
  }

  //пытаемся считать повторно сообщения в случае неудачи
  @KafkaListener(topics = "topic2", groupId = "group2", errorHandler = "retry")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    listen(consumerRecord, ack);
  }

  //пытаемся считать повторно сообщения в случае неудачи и/или фильтруем сообщение
  @KafkaListener(topics = "topic3", groupId = "group3", errorHandler = "retry", filter = "messageFilterDiscard")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    listen(consumerRecord, ack);
  }

  //логгируем --> пытаемся получить И обработать -->  отправляем ответ для Kafka
  private void listen(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack){
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    service.handle(consumerRecord.topic(), consumerRecord.value());
    ack.acknowledge();
  }
}
