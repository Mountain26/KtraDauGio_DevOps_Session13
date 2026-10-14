package com.example.seat.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Event phát sinh sau khi giữ chỗ thành công
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class SeatAllocationEvent {
    private String cinemaBookingId;
    private String movieCode;
    private String showTime;
    private List<String> seatNumbers;
    private String customerEmail;
    private Double totalPrice;
    private String status;
}
