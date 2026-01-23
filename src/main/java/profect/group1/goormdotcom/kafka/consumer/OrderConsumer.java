package profect.group1.goormdotcom.kafka.consumer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import org.springframework.transaction.annotation.Transactional;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import profect.group1.goormdotcom.kafka.common.EventEnvelope;
import profect.group1.goormdotcom.kafka.common.EventManager;
import profect.group1.goormdotcom.kafka.event.StockRollbackFailedEvent;
import profect.group1.goormdotcom.order.event.Stock.StockRollbackCompletedEvent;
import profect.group1.goormdotcom.order.service.OrderService;
import profect.group1.goormdotcom.kafka.event.DeliveryStartedEvent;
import profect.group1.goormdotcom.kafka.event.DeliveryStartFailedEvent;
import profect.group1.goormdotcom.order.domain.enums.OrderStatus;

import java.util.IllegalFormatConversionException;

@Slf4j
@Component
@RequiredArgsConstructor
@EnableKafka
public class OrderConsumer {
    private final ObjectMapper objectMapper;
    private final EventManager eventManager;
    private final OrderService orderService;

    /**
     * 재고 서비스에서 재고 롤백 완료 이벤트를 수신하여 주문 상태를 갱신한다.
     * 재고 롤백이 완료되었으므로 주문 상태를 FAILED로 변경한다.
     */
    @KafkaListener(
        topics = "order-service-topic",
        groupId = "order-service-cg"
    )
    public void handleStockEvent(String message) {
        try{
            EventEnvelope eventEnvelope = objectMapper.readValue(message, EventEnvelope.class);
            switch (eventEnvelope.getEventType()) {

                case "StockRollbackCompleted":
                    StockRollbackCompletedEvent stockRollbackCompletedEvent = eventManager.unwrap(eventEnvelope, StockRollbackCompletedEvent.class);
                    log.info("재고 롤백 완료 이벤트 수신: orderId={}", stockRollbackCompletedEvent.orderId());
                    // 주문 상태를 FAILED로 변경 (재고 롤백 완료 = 결제 실패 처리 완료)
                    orderService.appendOrderStatus(stockRollbackCompletedEvent.orderId(), OrderStatus.FAILED);
                    log.info("재고 롤백 완료에 따라 주문 상태를 FAILED로 갱신: orderId={}", stockRollbackCompletedEvent.orderId());
                    break;

                // Stock에서 DLQ 처리
//            case "StockRollbackFailedEvent":
//                StockRollbackFailedEvent stockRollbackFailedEvent = eventManager.unwrap(eventEnvelope, StockRollbackFailedEvent.class);
//                log.info("재고 롤백 실패 이벤트 수신: orderId={}", stockRollbackFailedEvent);
//                orderService.appendOrderStatus(stockRollbackFailedEvent.getOrderId(), OrderStatus.FAILED);
//                log.info("재고 롤백 실패에 따라 주문 상태를 FAILED로 갱신: orderId={}", stockRollbackFailedEvent.getOrderId());
//                break;

                case "DeliveryStarted":
                    DeliveryStartedEvent deliveryStartedEvent = eventManager.unwrap(eventEnvelope, DeliveryStartedEvent.class);
                    log.info("배송 시작 이벤트 수신: orderId={}", deliveryStartedEvent.getOrderId());
                    orderService.appendOrderStatus(deliveryStartedEvent.getOrderId(), OrderStatus.COMPLETED);
                    log.info("배송 완료에 따라 주문 상태를 COMPLETED로 갱신: orderId={}", deliveryStartedEvent.getOrderId());
                    break;

                case "DeliveryStartFailed":
                    DeliveryStartFailedEvent deliveryStartFailedEvent = eventManager.unwrap(eventEnvelope, DeliveryStartFailedEvent.class);
                    log.info("배송 시작 실패 이벤트 수신: orderId={}", deliveryStartFailedEvent.getOrderId());
                    orderService.appendOrderStatus(deliveryStartFailedEvent.getOrderId(), OrderStatus.FAILED);
                    log.info("배송 시작 실패에 따라 주문 상태를 FAILED로 갱신: orderId={}", deliveryStartFailedEvent.getOrderId());
                    break;

                default:
                    throw new IllegalStateException("Unexpected event type: " + eventEnvelope.getEventType());
            }
        } catch (JsonProcessingException e) {
            throw new IllegalStateException("Error parsing event envelope from Kafka message");
        }
    }

//    @KafkaListener(
//        topics = "order-service-from-delivery",
//        groupId = "order-service-cg"
//    )
//    public void handleDeliveryEvent(String message){
//        EventEnvelope eventEnvelope = objectMapper.convertValue(message, EventEnvelope.class);
//        switch (eventEnvelope.getEventType()) {
//            case "DeliveryStarted":
//                DeliveryStartedEvent deliveryStartedEvent = objectMapper.convertValue(eventEnvelope, DeliveryStartedEvent.class);
//                log.info("배송 시작 이벤트 수신: orderId={}", deliveryStartedEvent.getOrderId());
//                orderService.appendOrderStatus(deliveryStartedEvent.getOrderId(), OrderStatus.COMPLETED);
//                log.info("배송 완료에 따라 주문 상태를 COMPLETED로 갱신: orderId={}", deliveryStartedEvent.getOrderId());
//
//            case "DeliveryStartFailed":
//                DeliveryStartFailedEvent deliveryStartFailedEvent = objectMapper.convertValue(eventEnvelope, DeliveryStartFailedEvent.class);
//                log.info("배송 시작 실패 이벤트 수신: orderId={}", deliveryStartFailedEvent.getOrderId());
//                orderService.appendOrderStatus(deliveryStartFailedEvent.getOrderId(), OrderStatus.FAILED);
//                log.info("배송 시작 실패에 따라 주문 상태를 FAILED로 갱신: orderId={}", deliveryStartFailedEvent.getOrderId());
//
//            default:
//                throw new IllegalStateException("Unexpected event type: " + eventEnvelope.getEventType());
//        }
//    }
}