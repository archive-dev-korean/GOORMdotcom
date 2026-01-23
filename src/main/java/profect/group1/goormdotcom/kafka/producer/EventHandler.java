package profect.group1.goormdotcom.kafka.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Component;
import org.springframework.transaction.event.TransactionPhase;
import org.springframework.transaction.event.TransactionalEventListener;
import profect.group1.goormdotcom.kafka.event.DeliveryStartedEvent;
import profect.group1.goormdotcom.kafka.event.DeliveryStartFailedEvent;

import java.time.LocalDateTime;

@Slf4j
@Component
@RequiredArgsConstructor
public class EventHandler {
    private final DeliveryProducer deliveryProducer;

    @TransactionalEventListener(phase = TransactionPhase.AFTER_COMMIT)
    @Async
    public void handleDeliveryStartedEvent(DeliveryStartedEvent event) {
        log.info("배송 시작 이벤트 발행: orderId={}, deliveryId={}", event.getOrderId(), event.getDeliveryId());
        deliveryProducer.send(
                "order-service-topic",
                event.getOrderId().toString(),
                "DeliveryStarted",
                "Delivery",
                event.getEventTime(),
                1,
                "Delivery",
                event);
    }
    @TransactionalEventListener(phase = TransactionPhase.AFTER_ROLLBACK)
    @Async
    public void handleDeliveryStartFailedEvent(DeliveryStartFailedEvent event) {
        log.info("배송 시작 실패 이벤트 발행: orderId={}", event.getOrderId());
        deliveryProducer.send(
                "order-service-topic",
                event.getOrderId().toString(),
                "DeliveryStarted",
                "Delivery",
                event.getEventTime(),
                1,
                "Delivery",
                event);
    }
}