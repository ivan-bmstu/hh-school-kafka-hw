package ru.hh.kafkahw.consumer;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.springframework.kafka.listener.adapter.RecordFilterStrategy;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;

/*
Класс, который определяет стратегию фильтрации сообщений для consumers. Если метод filter возвращает true, тогда
обработка этого сообщения прерывается, на стороне kafka происходит смещения указателя для данного топика и партиции,
и при последующем вытягивании сообщения consumer попробует считать следующее сообщение. Фильтр задается как параметр
в аннотации @KafkaListener(filter="springBeanName")
 */
@Component
class MessageFilterDiscard<K, V> implements RecordFilterStrategy<K, V> {

    private final Service service;

    public MessageFilterDiscard(Service service) {
        this.service = service;
    }

    @Override
    public boolean filter(ConsumerRecord<K, V> consumerRecord) {
        //мы не считываем сообщение, если сообщение с таким содержимом было считано ранее из этого же topic
        //проверяем по наличию записи в "БД" (класс Service, потоко-безопасная ConcurrentMap)
        return service.count(consumerRecord.topic(), consumerRecord.value().toString()) != 0;
    }
}
