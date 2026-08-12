package com.fooddelivery.order.web;

import com.fooddelivery.order.dto.admin.ConsumerGroupLagResponse;
import com.fooddelivery.order.dto.admin.ReplayRequest;
import com.fooddelivery.order.dto.admin.ReplayResponse;
import com.fooddelivery.order.dto.admin.TopicInfoResponse;
import com.fooddelivery.order.service.KafkaAdminService;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.web.bind.annotation.*;

import java.util.List;

/**
 * Kafka admin/replay endpoints (ARCHITECTURE.md section 8), consumed by the frontend's
 * "Admin / Replay" panel. Backed entirely by the raw Kafka {@code AdminClient} in
 * {@link KafkaAdminService}.
 */
@RestController
@RequestMapping("/api/admin/kafka")
@RequiredArgsConstructor
public class KafkaAdminController {

    private final KafkaAdminService kafkaAdminService;

    @GetMapping("/topics")
    public List<TopicInfoResponse> topics() {
        return kafkaAdminService.listTopics();
    }

    @GetMapping("/consumer-groups")
    public List<ConsumerGroupLagResponse> consumerGroups() {
        return kafkaAdminService.listConsumerGroups();
    }

    @PostMapping("/replay")
    public ReplayResponse replay(@Valid @RequestBody ReplayRequest request) {
        return kafkaAdminService.replay(request.getListenerId());
    }
}
