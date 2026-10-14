package com.example.movie.service;

import com.example.movie.model.CinemaBookingRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

@Service
@Slf4j
@RequiredArgsConstructor
public class BookingPublisherService {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Nhận yêu cầu đặt vé, sinh correlationId duy nhất và bắn event lên Kafka kèm correlationId trong Header.
     *
     * @param request Dữ liệu đặt vé từ Client
     * @return correlationId đã được sinh cho giao dịch này
     */
    public String createBooking(CinemaBookingRequest request) {
        // Bước 1: Sinh Correlation ID ngẫu nhiên (UUID) để định danh xuyên suốt luồng Saga
        String correlationId = UUID.randomUUID().toString();

        // Ghi log khởi tạo giao dịch với Correlation ID
        log.info("[MovieBookingService] Created booking {}. CorrelationID: {}", 
                request.getCinemaBookingId(), correlationId);

        try {
            // Chuyển payload sang JSON string
            String payload = objectMapper.writeValueAsString(request);

            // Tạo ProducerRecord gửi đến topic "booking-events" với key là cinemaBookingId
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "booking-events",
                    request.getCinemaBookingId(),
                    payload
            );

            // Bước 2: GẮN correlationId VÀO HEADER CỦA TIN NHẮN (KHÔNG đặt trong payload)
            // Giúp tách biệt thông tin quản trị/tracing với nghiệp vụ, tránh làm bẩn payload
            record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));

            // Gửi tin nhắn lên Kafka Broker
            kafkaTemplate.send(record);

        } catch (JsonProcessingException e) {
            log.error("[MovieBookingService] Error serializing booking request: {}", e.getMessage(), e);
        }

        return correlationId;
    }
}
