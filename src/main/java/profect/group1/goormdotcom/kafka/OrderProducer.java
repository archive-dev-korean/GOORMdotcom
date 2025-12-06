package profect.group1.goormdotcom.kafka;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import profect.group1.goormdotcom.order.event.Delivery.DeliveryRequestedEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import profect.group1.goormdotcom.order.event.Stock.StockRollbackRequestedEvent;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public void send(String topic, DeliveryRequestedEvent event) {
        kafkaTemplate.send(topic, event);
        log.info("Kafka 메시지 발행 완료: topic={}, orderId={}", topic, event.orderId());
    }

    public void send(String topic, StockRollbackRequestedEvent event) {
        kafkaTemplate.send(topic, event);
        log.info("Kafka 메시지 발행 완료: topic={}, orderId={}", topic, event.orderId());
    }
}   