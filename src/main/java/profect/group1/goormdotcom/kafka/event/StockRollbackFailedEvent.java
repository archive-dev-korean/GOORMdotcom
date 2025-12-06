package profect.group1.goormdotcom.kafka.event;

import java.util.UUID;

public record StockRollbackFailedEvent(
    UUID orderId,
    String errorMessage,
    String errorType
) {
}