package profect.group1.goormdotcom.kafka.event;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;

import java.time.Instant;
import java.util.UUID;

@Getter
@AllArgsConstructor
@NoArgsConstructor
public class DeliveryRequestedEvent {
    private UUID orderId;
    private UUID customerId;
    private String address;
    private String addressDetail;
    private String zipcode;
    private String phone;
    private String name;
    private String deliveryMemo;
}

