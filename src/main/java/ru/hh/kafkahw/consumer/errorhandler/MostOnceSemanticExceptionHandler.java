package ru.hh.kafkahw.consumer.errorhandler;

import org.apache.kafka.clients.consumer.Consumer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.listener.ManualAckListenerErrorHandler;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;


/*
Класс обработчик исключений. Перехватывает исключения у @KafkaListener (если указан в параметре errorHandler). Для
соблюдения семантики atMostOnce повторно сообщения считываться не будут, принудительно вызывается ack.acknowledge()
 */
@Component("mostOnceSemantic")
public class MostOnceSemanticExceptionHandler implements ManualAckListenerErrorHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(MostOnceSemanticExceptionHandler.class);

    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception, Consumer<?, ?> consumer, Acknowledgment ack) {
        if (ack != null) {
            ack.acknowledge();
        }
        LOGGER.error("FAILED AT MESSAGE '{}' ERROR OCCUR: {}", message.getPayload(), exception.getMessage());
        return message; // return only for @ToSend annotation
    }
}
