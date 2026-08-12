package com.fooddelivery.payment.listener;

import com.fooddelivery.payment.event.OrderEvent;
import com.fooddelivery.payment.service.PaymentService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

/**
 * Consumes order-events. The `id` attribute below must equal the consumer group id exactly
 * (ARCHITECTURE.md section 3) - order-service's replay API looks up this listener container by
 * that id via KafkaListenerEndpointRegistry.
 *
 * order-events carries three event types (ORDER_PLACED / ORDER_COMPLETED / ORDER_CANCELLED);
 * payment-service only acts on ORDER_PLACED and ignores the other two.
 */
@Component
public class OrderEventListener {

    private static final Logger log = LoggerFactory.getLogger(OrderEventListener.class);

    private final PaymentService paymentService;

    public OrderEventListener(PaymentService paymentService) {
        this.paymentService = paymentService;
    }

    @KafkaListener(id = "payment-service-group", groupId = "payment-service-group", topics = "order-events")
    public void onOrderEvent(OrderEvent event) {
        // A malformed/unparseable message never reaches this method at all - it's caught earlier
        // by the ErrorHandlingDeserializer + DefaultErrorHandler configured in
        // KafkaConsumerConfig/application.yml. This try/catch guards against everything else that
        // can go wrong once we *do* have a valid OrderEvent object (e.g. a transient DB outage,
        // or a null/unexpected field) so that one bad event can never kill this listener's thread
        // and stop the whole consumer group from making progress.
        try {
            if (event == null) {
                log.warn("Received null OrderEvent payload - skipping");
                return;
            }
            if (!"ORDER_PLACED".equals(event.getEventType())) {
                log.debug("Ignoring order-events event of type {} for order {}", event.getEventType(), event.getOrderId());
                return;
            }

            log.info("Received ORDER_PLACED for order {} (userId={}, amount={})",
                    event.getOrderId(), event.getUserId(), event.getPaymentAmount());
            paymentService.handleOrderPlaced(event);
        } catch (Exception ex) {
            log.error("Unexpected error handling order-events message (orderId={}): {}",
                    event != null ? event.getOrderId() : "unknown", ex.getMessage(), ex);
        }
    }
}
