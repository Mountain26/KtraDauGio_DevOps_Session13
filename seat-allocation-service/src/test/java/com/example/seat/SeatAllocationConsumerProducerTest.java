package com.example.seat;

import com.example.seat.consumer.BookingEventConsumer;
import com.example.seat.model.CinemaBookingRequest;
import com.example.seat.producer.SeatConfirmedProducer;
import com.example.seat.service.SeatAllocationService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class SeatAllocationConsumerProducerTest {

    @Mock
    private SeatAllocationService seatService;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @Test
    @DisplayName("Kiểm tra BookingEventConsumer trích xuất correlationId từ header và SeatConfirmedProducer truyền tiếp correlationId")
    void testConsumerAndProducerFlow() throws Exception {
        SeatConfirmedProducer seatConfirmedProducer = new SeatConfirmedProducer(kafkaTemplate, objectMapper);
        BookingEventConsumer consumer = new BookingEventConsumer(seatService, seatConfirmedProducer, objectMapper);

        String correlationId = "550e8400-e29b-41d4-a716-446655440000";
        CinemaBookingRequest request = CinemaBookingRequest.builder()
                .cinemaBookingId("CIN-2024-789")
                .movieCode("AVENGERS-5")
                .showTime("2024-12-25T19:30:00")
                .seatNumbers(List.of("A12", "A13"))
                .customerEmail("tuananh@email.com")
                .totalPrice(240000.0)
                .build();

        String payload = objectMapper.writeValueAsString(request);
        RecordHeaders headers = new RecordHeaders();
        headers.add(new RecordHeader("correlationId", correlationId.getBytes(StandardCharsets.UTF_8)));

        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(
                "booking-events", 0, 0L, "CIN-2024-789", payload
        );
        consumerRecord.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));

        when(seatService.reserveSeats(request)).thenReturn("A12, A13");

        // Gọi consumer xử lý record
        consumer.handleBooking(consumerRecord);

        // Kiểm tra logic giữ ghế được gọi
        verify(seatService).reserveSeats(request);

        // Kiểm tra sự kiện tiếp theo được gửi đi với correlationId trong header
        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());

        ProducerRecord<String, String> capturedRecord = captor.getValue();
        assertEquals("seat-confirmed-events", capturedRecord.topic());
        assertEquals("CIN-2024-789", capturedRecord.key());

        var nextHeader = capturedRecord.headers().lastHeader("correlationId");
        assertNotNull(nextHeader);
        assertEquals(correlationId, new String(nextHeader.value(), StandardCharsets.UTF_8));
    }
}
