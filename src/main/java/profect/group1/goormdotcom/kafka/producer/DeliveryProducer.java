package profect.group1.goormdotcom.kafka;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import profect.group1.goormdotcom.kafka.event.DeliveryStartFailedEvent;
import profect.group1.goormdotcom.kafka.event.DeliveryStartedEvent;
import profect.group1.goormdotcom.order.event.Delivery.DeliveryRequestedEvent;

@Slf4j
@Component
@RequiredArgsConstructor
public class DeliveryProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * 배송 요청 이벤트를 Kafka 토픽으로 발행
     * @param topic Kafka 토픽 이름
     * @param event 배송 요청 이벤트
     */
    public void sendDeliveryStartedEvent(String topic, DeliveryStartedEvent event) {
        kafkaTemplate.send(topic, event);
        log.info("Kafka 메시지 발행 완료: topic={}, orderId={}", topic, event.orderId());
    }

    public void sendDeliveryStartFailedEvent(String topic, DeliveryStartFailedEvent event) {
        kafkaTemplate.send(topic, event);
        log.info("Kafka 메시지 발행 완료: topic={}, orderId={}", topic, event.orderId());
    }
}