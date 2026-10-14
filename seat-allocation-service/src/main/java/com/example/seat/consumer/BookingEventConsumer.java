package com.example.seat.consumer;

import com.example.seat.model.CinemaBookingRequest;
import com.example.seat.producer.SeatConfirmedProducer;
import com.example.seat.service.SeatAllocationService;
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
public class BookingEventConsumer {

    private final SeatAllocationService seatService;
    private final SeatConfirmedProducer seatConfirmedProducer;
    private final ObjectMapper objectMapper;

    /**
     * Lắng nghe sự kiện booking-events từ MovieBookingService
     */
    @KafkaListener(topics = "booking-events", groupId = "seat-group")
    public void handleBooking(ConsumerRecord<String, String> record) {
        // TRÍCH XUẤT correlationId TỪ HEADER - KHÔNG LẤY TỪ PAYLOAD
        Header header = record.headers().lastHeader("correlationId");
        String correlationId = (header != null && header.value() != null)
                ? new String(header.value(), StandardCharsets.UTF_8)
                : "UNKNOWN";

        // Ghi log tiếp nhận yêu cầu với Correlation ID trích xuất từ header
        log.info("[SeatAllocationService] Received SeatRequest for {}. CorrelationID: {}", 
                record.key(), correlationId);

        try {
            // Deserialize nội dung payload thành CinemaBookingRequest
            CinemaBookingRequest request = objectMapper.readValue(record.value(), CinemaBookingRequest.class);

            // Thực hiện nghiệp vụ giữ ghế
            String seatResult = seatService.reserveSeats(request);

            // Ghi log kết quả giữ ghế với Correlation ID
            log.info("[SeatAllocationService] Seat reserved: {}. CorrelationID: {}", 
                    seatResult, correlationId);

            // Gửi sự kiện tiếp theo (seat-confirmed-events) kèm Correlation ID truyền tiếp trong header
            seatConfirmedProducer.publishSeatConfirmed(request, correlationId);

        } catch (Exception e) {
            log.error("[SeatAllocationService] Error processing booking: {}", e.getMessage(), e);
        }
    }
}
