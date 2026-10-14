package com.example.payment.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class PaymentEvent {
    private String paymentId;
    private String cinemaBookingId;
    private String customerEmail;
    private Double amount;
    private String status;
    private LocalDateTime transactionTime;
}
