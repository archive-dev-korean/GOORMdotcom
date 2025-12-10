package profect.group1.goormdotcom.kafka.producer;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import profect.group1.goormdotcom.kafka.common.EventEnvelope;
import profect.group1.goormdotcom.kafka.common.EventManager;
import profect.group1.goormdotcom.order.event.Delivery.DeliveryRequestedEvent;
import com.fasterxml.jackson.databind.ObjectMapper;
import profect.group1.goormdotcom.order.event.Stock.StockRollbackRequestedEvent;

import java.time.LocalDateTime;
import java.util.UUID;

@Slf4j
@Component
@RequiredArgsConstructor
public class OrderProducer {
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final EventManager eventManager;

    public void send(
            String topic,
            String key,
            String eventType,
            String aggregateType,
            LocalDateTime occuredAt,
            long version,
            String source,
            Object eventPayload) {

        EventEnvelope eventEnvelope = eventManager.wrap(eventPayload, eventType, aggregateType, occuredAt, version, source);
        kafkaTemplate.send(topic, key, eventEnvelope);
        log.info("Kafka 메시지 발행 완료: topic={} payload={}", topic, eventEnvelope.getPayload());
    }
}   