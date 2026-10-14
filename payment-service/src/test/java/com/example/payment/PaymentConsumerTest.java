package com.example.payment;

import com.example.payment.consumer.SeatConfirmedConsumer;
import com.example.payment.model.SeatAllocationEvent;
import com.example.payment.service.PaymentProcessingService;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;

import java.nio.charset.StandardCharsets;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class PaymentConsumerTest {

    @Mock
    private PaymentProcessingService paymentService;

    @Spy
    private ObjectMapper objectMapper = new ObjectMapper();

    @InjectMocks
    private SeatConfirmedConsumer seatConfirmedConsumer;

    @Test
    @DisplayName("Kiểm tra SeatConfirmedConsumer nhận event và trích xuất correlationId để xử lý thanh toán")
    void testHandleSeatConfirmed() throws Exception {
        String correlationId = "550e8400-e29b-41d4-a716-446655440000";
        SeatAllocationEvent event = SeatAllocationEvent.builder()
                .cinemaBookingId("CIN-2024-789")
                .movieCode("AVENGERS-5")
                .showTime("2024-12-25T19:30:00")
                .seatNumbers(List.of("A12", "A13"))
                .customerEmail("tuananh@email.com")
                .totalPrice(240000.0)
                .status("SEAT_RESERVED")
                .build();

        String payload = objectMapper.writeValueAsString(event);
        ConsumerRecord<String, String> consumerRecord = new ConsumerRecord<>(
                "seat-confirmed-events", 0, 0L, "CIN-2024-789", payload
        );
        consumerRecord.headers().add("correlationId", correlationId.getBytes(StandardCharsets.UTF_8));

        when(paymentService.processPayment(any(SeatAllocationEvent.class))).thenReturn(true);

        seatConfirmedConsumer.handleSeatConfirmed(consumerRecord);

        verify(paymentService).processPayment(any(SeatAllocationEvent.class));
    }
}
