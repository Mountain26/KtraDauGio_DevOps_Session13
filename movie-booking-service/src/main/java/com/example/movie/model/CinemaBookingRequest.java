package com.example.movie.model;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.util.List;

/**
 * Model biểu diễn yêu cầu đặt vé từ client.
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class CinemaBookingRequest {
    private String cinemaBookingId;
    private String movieCode;
    private String showTime;
    private List<String> seatNumbers;
    private String customerEmail;
    private Double totalPrice;
}
