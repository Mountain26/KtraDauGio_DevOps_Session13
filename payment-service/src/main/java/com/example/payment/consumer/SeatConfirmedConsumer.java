package com.example.payment.consumer;

import com.example.payment.model.SeatAllocationEvent;
import com.example.payment.service.PaymentProcessingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@Slf4j
@RequiredArgsConstructor
public class SeatConfirmedConsumer {

    private final PaymentProcessingService paymentService;
    private final ObjectMapper objectMapper;

    /**
     * Lắng nghe sự kiện seat-confirmed-events từ SeatAllocationService
     */
    @KafkaListener(topics = "seat-confirmed-events", groupId = "payment-group")
    public void handleSeatConfirmed(ConsumerRecord<String, String> record) {
        // TRÍCH XUẤT correlationId TỪ HEADER - KHÔNG LẤY TỪ PAYLOAD
        Header header = record.headers().lastHeader("correlationId");
        String correlationId = (header != null && header.value() != null)
                ? new String(header.value(), StandardCharsets.UTF_8)
                : "UNKNOWN";

        // Ghi log xử lý thanh toán với Correlation ID
        log.info("[PaymentService] Processing Payment for {}. CorrelationID: {}", 
                record.key(), correlationId);

        try {
            // Deserialize nội dung payload thành SeatAllocationEvent
            SeatAllocationEvent event = objectMapper.readValue(record.value(), SeatAllocationEvent.class);

            // Xử lý thanh toán
            boolean success = paymentService.processPayment(event);

            if (success) {
                // Hiển thị số tiền thanh toán (ví dụ: 240000 VND)
                long price = event.getTotalPrice() != null ? event.getTotalPrice().longValue() : 0L;
                log.info("[PaymentService] Payment success: {} VND. CorrelationID: {}", 
                        price, correlationId);
            }

        } catch (Exception e) {
            log.error("[PaymentService] Error processing payment: {}", e.getMessage(), e);
        }
    }
}
