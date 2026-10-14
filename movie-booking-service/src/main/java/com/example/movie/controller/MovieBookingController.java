package com.example.movie.controller;

import com.example.movie.model.CinemaBookingRequest;
import com.example.movie.service.BookingPublisherService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/bookings")
@RequiredArgsConstructor
public class MovieBookingController {

    private final BookingPublisherService bookingPublisherService;

    /**
     * API tiếp nhận yêu cầu đặt vé xem phim trực tuyến từ client
     */
    @PostMapping
    public ResponseEntity<Map<String, Object>> createBooking(@RequestBody CinemaBookingRequest request) {
        String correlationId = bookingPublisherService.createBooking(request);

        return ResponseEntity.ok(Map.of(
                "status", "SUCCESS",
                "message", "Yêu cầu đặt vé đã được tiếp nhận và xử lý theo luồng Saga",
                "cinemaBookingId", request.getCinemaBookingId(),
                "correlationId", correlationId
        ));
    }
}
