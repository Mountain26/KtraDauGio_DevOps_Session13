package com.example.seat.producer;

import com.example.seat.model.CinemaBookingRequest;
import com.example.seat.model.SeatAllocationEvent;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.nio.charset.StandardCharsets;

@Component
@Slf4j
@RequiredArgsConstructor
public class SeatConfirmedProducer {

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    /**
     * Bắn event xác nhận giữ ghế thành công kèm theo Correlation ID được kế thừa từ bước trước trong Header.
     *
     * @param request Thông tin đặt vé
     * @param correlationId Correlation ID nhận được từ event trước đó
     */
    public void publishSeatConfirmed(CinemaBookingRequest request, String correlationId) {
        try {
            // Khởi tạo Event giữ ghế thành công
            SeatAllocationEvent event = SeatAllocationEvent.builder()
                    .cinemaBookingId(request.getCinemaBookingId())
                    .movieCode(request.getMovieCode())
                    .showTime(request.getShowTime())
                    .seatNumbers(request.getSeatNumbers())
                    .customerEmail(request.getCustomerEmail())
                    .totalPrice(request.getTotalPrice())
                    .status("SEAT_RESERVED")
                    .build();

            String payload = objectMapper.writeValueAsString(event);

            // Tạo ProducerRecord gửi đến topic "seat-confirmed-events"
            ProducerRecord<String, String> record = new ProducerRecord<>(
                    "seat-confirmed-events",
                    request.getCinemaBookingId(),
                    payload
            );

            // GẮN correlationId TIẾP TỤC VÀO HEADER ĐỂ TRUYỀN CHO SERVICE TIẾP THEO (PaymentService)
            record.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));

            kafkaTemplate.send(record);

        } catch (JsonProcessingException e) {
            log.error("[SeatAllocationService] Error publishing seat confirmed event: {}", e.getMessage(), e);
        }
    }
}
