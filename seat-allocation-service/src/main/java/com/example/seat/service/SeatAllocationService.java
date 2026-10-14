package com.example.seat.service;

import com.example.seat.model.CinemaBookingRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class SeatAllocationService {

    /**
     * Thực hiện nghiệp vụ giữ ghế trong hệ thống phòng vé
     *
     * @param request Thông tin yêu cầu đặt vé
     * @return Danh sách ghế đã được giữ thành công (ví dụ: "A12, A13")
     */
    public String reserveSeats(CinemaBookingRequest request) {
        if (request.getSeatNumbers() == null || request.getSeatNumbers().isEmpty()) {
            return "NO_SEATS";
        }
        // Giả lập logic kiểm tra và khóa ghế thành công trong database
        return String.join(", ", request.getSeatNumbers());
    }
}
