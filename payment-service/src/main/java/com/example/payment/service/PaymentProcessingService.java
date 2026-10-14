package com.example.payment.service;

import com.example.payment.model.SeatAllocationEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class PaymentProcessingService {

    /**
     * Giả lập xử lý thanh toán tự động qua cổng thanh toán
     *
     * @param event Dữ liệu sự kiện giữ chỗ từ SeatAllocationService
     * @return true nếu thanh toán thành công
     */
    public boolean processPayment(SeatAllocationEvent event) {
        if (event.getTotalPrice() == null || event.getTotalPrice() <= 0) {
            log.warn("[PaymentService] Invalid total price for booking: {}", event.getCinemaBookingId());
            return false;
        }

        // Giả lập thanh toán qua cổng thanh toán (Banking / VNPAY / Momo)
        return true;
    }
}
