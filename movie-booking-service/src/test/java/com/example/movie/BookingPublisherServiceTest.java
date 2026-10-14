package com.example.movie;

import com.example.movie.model.CinemaBookingRequest;
import com.example.movie.service.BookingPublisherService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.ProducerRecord;
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

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;

@ExtendWith(MockitoExtension.class)
class BookingPublisherServiceTest {

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private BookingPublisherService bookingPublisherService;

    @Test
    @DisplayName("Kiểm tra createBooking sinh correlationId và gắn vào Header của ProducerRecord")
    void testCreateBooking_AttachesCorrelationIdToHeader() {
        CinemaBookingRequest request = CinemaBookingRequest.builder()
                .cinemaBookingId("CIN-2024-789")
                .movieCode("AVENGERS-5")
                .showTime("2024-12-25T19:30:00")
                .seatNumbers(List.of("A12", "A13"))
                .customerEmail("tuananh@email.com")
                .totalPrice(240000.0)
                .build();

        String correlationId = bookingPublisherService.createBooking(request);

        assertNotNull(correlationId, "Correlation ID không được rỗng");

        @SuppressWarnings("unchecked")
        ArgumentCaptor<ProducerRecord<String, String>> captor = ArgumentCaptor.forClass(ProducerRecord.class);
        verify(kafkaTemplate).send(captor.capture());

        ProducerRecord<String, String> capturedRecord = captor.getValue();
        assertEquals("booking-events", capturedRecord.topic());
        assertEquals("CIN-2024-789", capturedRecord.key());

        var header = capturedRecord.headers().lastHeader("correlationId");
        assertNotNull(header, "Header phải chứa correlationId");
        assertEquals(correlationId, new String(header.value(), StandardCharsets.UTF_8));
    }
}
