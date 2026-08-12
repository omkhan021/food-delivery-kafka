package com.fooddelivery.order.service;

import com.fooddelivery.order.domain.EventLog;
import com.fooddelivery.order.domain.Order;
import com.fooddelivery.order.domain.OrderItem;
import com.fooddelivery.order.domain.OrderStatus;
import com.fooddelivery.order.domain.OrderStatusHistory;
import com.fooddelivery.order.dto.api.CreateOrderRequest;
import com.fooddelivery.order.dto.api.EventLogResponse;
import com.fooddelivery.order.dto.api.OrderResponse;
import com.fooddelivery.order.dto.api.StatusStreamEvent;
import com.fooddelivery.order.dto.events.FoodItem;
import com.fooddelivery.order.dto.events.OrderEvent;
import com.fooddelivery.order.exception.OrderNotFoundException;
import com.fooddelivery.order.repository.EventLogRepository;
import com.fooddelivery.order.repository.OrderItemRepository;
import com.fooddelivery.order.repository.OrderRepository;
import com.fooddelivery.order.repository.OrderStatusHistoryRepository;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * Core order CRUD / read-model logic. order-service is the saga <b>initiator</b>: creating an
 * order here is what kicks off the entire cross-service Kafka flow described in
 * ARCHITECTURE.md section 5, by publishing {@code ORDER_PLACED} to {@code order-events}.
 */
@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository orderRepository;
    private final OrderItemRepository orderItemRepository;
    private final OrderStatusHistoryRepository historyRepository;
    private final EventLogRepository eventLogRepository;
    private final OrderEventProducer orderEventProducer;
    private final SseEmitterService sseEmitterService;

    @Transactional
    public OrderResponse createOrder(CreateOrderRequest request) {
        String orderId = generateOrderId();
        Instant now = Instant.now();

        BigDecimal computedTotal = request.getFoodItems().stream()
                .map(i -> nullToZero(i.getPrice()).multiply(BigDecimal.valueOf(i.getQuantity())))
                .reduce(BigDecimal.ZERO, BigDecimal::add);
        BigDecimal paymentAmount = request.getPaymentAmount() != null ? request.getPaymentAmount() : computedTotal;

        LocalDate orderDate = request.getOrderDate() != null ? request.getOrderDate() : LocalDate.now();
        LocalTime orderTime = request.getOrderTime() != null ? request.getOrderTime() : LocalTime.now();

        Order order = new Order();
        order.setOrderId(orderId);
        order.setUserId(request.getUserId());
        order.setPaymentAmount(paymentAmount);
        order.setStatus(OrderStatus.PLACED);
        order.setOrderDate(orderDate);
        order.setOrderTime(orderTime);
        orderRepository.save(order);

        List<OrderItem> items = request.getFoodItems().stream().map(i -> {
            OrderItem item = new OrderItem();
            item.setOrderId(orderId);
            item.setItemName(i.getItemName());
            item.setQuantity(i.getQuantity());
            item.setPrice(nullToZero(i.getPrice()));
            return item;
        }).collect(Collectors.toList());
        orderItemRepository.saveAll(items);

        appendHistory(orderId, OrderStatus.PLACED.name(), null, "Order placed");

        List<FoodItem> foodItemEvents = items.stream()
                .map(i -> new FoodItem(i.getItemName(), i.getQuantity(), i.getPrice()))
                .collect(Collectors.toList());

        OrderEvent event = OrderEvent.builder()
                .eventId(UUID.randomUUID().toString())
                .eventType("ORDER_PLACED")
                .orderId(orderId)
                .timestamp(now)
                .userId(request.getUserId())
                .paymentAmount(paymentAmount)
                .foodItems(foodItemEvents)
                .orderDate(orderDate)
                .orderTime(orderTime)
                .build();
        orderEventProducer.publish(event);

        return toResponse(order, items, null);
    }

    @Transactional(readOnly = true)
    public List<OrderResponse> listOrders() {
        return orderRepository.findAllNewestFirst().stream()
                .map(o -> toResponse(o, orderItemRepository.findByOrderId(o.getOrderId()), null))
                .collect(Collectors.toList());
    }

    @Transactional(readOnly = true)
    public OrderResponse getOrder(String orderId) {
        Order order = orderRepository.findById(orderId).orElseThrow(() -> new OrderNotFoundException(orderId));
        List<OrderItem> items = orderItemRepository.findByOrderId(orderId);
        List<OrderStatusHistory> history = historyRepository.findByOrderIdOrderByCreatedAtAsc(orderId);
        return toResponse(order, items, history);
    }

    @Transactional(readOnly = true)
    public List<EventLogResponse> getEventLog(String orderId) {
        return eventLogRepository.findByOrderIdOrderByFirstSeenAtAsc(orderId).stream()
                .map(this::toEventLogResponse)
                .collect(Collectors.toList());
    }

    /**
     * Applies a status transition + history row + SSE push. Called by
     * {@code KafkaEventConsumerService} only for genuinely new (non-replayed) events.
     */
    @Transactional
    public void applyStatusTransition(String orderId, String newStatus, String sourceTopic, String note) {
        Optional<Order> maybeOrder = orderRepository.findById(orderId);
        if (maybeOrder.isEmpty()) {
            log.warn("Received event for unknown order {} (status={}, topic={}); recording history only if possible",
                    orderId, newStatus, sourceTopic);
            return;
        }
        Order order = maybeOrder.get();
        if (newStatus != null) {
            order.setStatus(OrderStatus.valueOf(newStatus));
            orderRepository.save(order);
        }
        appendHistory(orderId, newStatus != null ? newStatus : order.getStatus().name(), sourceTopic, note);
    }

    private void appendHistory(String orderId, String status, String sourceTopic, String note) {
        OrderStatusHistory history = new OrderStatusHistory();
        history.setOrderId(orderId);
        history.setStatus(status);
        history.setSourceTopic(sourceTopic);
        history.setNote(note);
        historyRepository.save(history);

        sseEmitterService.push(orderId, StatusStreamEvent.builder()
                .orderId(orderId)
                .status(status)
                .note(note)
                .timestamp(Instant.now())
                .build());
    }

    private String generateOrderId() {
        return "ORD-" + UUID.randomUUID().toString().replace("-", "").substring(0, 8).toUpperCase();
    }

    private BigDecimal nullToZero(BigDecimal value) {
        return value == null ? BigDecimal.ZERO : value;
    }

    private OrderResponse toResponse(Order order, List<OrderItem> items, List<OrderStatusHistory> history) {
        return OrderResponse.builder()
                .orderId(order.getOrderId())
                .userId(order.getUserId())
                .paymentAmount(order.getPaymentAmount())
                .status(order.getStatus().name())
                .orderDate(order.getOrderDate())
                .orderTime(order.getOrderTime())
                .createdAt(order.getCreatedAt())
                .updatedAt(order.getUpdatedAt())
                .items(items == null ? List.of() : items.stream()
                        .map(i -> OrderResponse.OrderItemResponse.builder()
                                .itemName(i.getItemName())
                                .quantity(i.getQuantity())
                                .price(i.getPrice())
                                .build())
                        .collect(Collectors.toList()))
                .statusHistory(history == null ? null : history.stream()
                        .map(h -> OrderResponse.OrderStatusHistoryResponse.builder()
                                .status(h.getStatus())
                                .sourceTopic(h.getSourceTopic())
                                .note(h.getNote())
                                .createdAt(h.getCreatedAt())
                                .build())
                        .collect(Collectors.toList()))
                .build();
    }

    private EventLogResponse toEventLogResponse(EventLog e) {
        return EventLogResponse.builder()
                .id(e.getId())
                .topic(e.getTopic())
                .partition(e.getPartition())
                .kafkaOffset(e.getKafkaOffset())
                .eventKey(e.getEventKey())
                .eventType(e.getEventType())
                .orderId(e.getOrderId())
                .payload(e.getPayload())
                .timesSeen(e.getTimesSeen())
                .firstSeenAt(e.getFirstSeenAt())
                .lastSeenAt(e.getLastSeenAt())
                .build();
    }
}
