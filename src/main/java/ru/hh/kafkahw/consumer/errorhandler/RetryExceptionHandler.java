package ru.hh.kafkahw.consumer.errorhandler;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.listener.KafkaListenerErrorHandler;
import org.springframework.kafka.listener.ListenerExecutionFailedException;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.Message;
import org.springframework.stereotype.Component;

/*
Класс обработчик исключений. Перехватывает исключения у @KafkaListener (если указан в параметре errorHandler). Для
соблюдения семантики atMostOnce и exactlyOnce в случае неудачи нужно повторно считывать сообщения,
для этого пробрасываем исключение, чтобы метод seek сместил current для Listener
 */
@Component("retry")
public class RetryExceptionHandler implements KafkaListenerErrorHandler {

    private final Logger LOGGER = LoggerFactory.getLogger(RetryExceptionHandler.class);


    @Override
    public Object handleError(Message<?> message, ListenerExecutionFailedException exception) {
        LOGGER.error("FAILED AT MESSAGE '{}' ERROR OCCUR: {}", message.getPayload(), exception.getMessage());
        // если это 9 попытка retry, то не пробрасываем исключение ==> не считываем сообщение повторно
        if (message.getHeaders().get(KafkaHeaders.DELIVERY_ATTEMPT, Integer.class) > 9){
            return "FAILED";
        }
        throw new RuntimeException();
    }
}
