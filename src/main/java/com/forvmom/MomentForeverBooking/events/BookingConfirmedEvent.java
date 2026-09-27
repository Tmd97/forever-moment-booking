package com.forvmom.MomentForeverBooking.events;

import java.time.LocalDateTime;

public class BookingConfirmedEvent extends BaseEvent implements OutboundEvent {

    private String bookingId;
    private Long userId;
    private String userEmail;
    private Long experienceId;
    private Long timeSlotMapperId;
    private Integer guestCount;
    private LocalDateTime confirmedAt;

    public BookingConfirmedEvent() {
    }

    public BookingConfirmedEvent(String bookingId,
                                 Long userId,
                                 String userEmail,
                                 Long experienceId,
                                 Long timeSlotMapperId,
                                 Integer guestCount,
                                 LocalDateTime confirmedAt) {
        this.bookingId = bookingId;
        this.userId = userId;
        this.userEmail = userEmail;
        this.experienceId = experienceId;
        this.timeSlotMapperId = timeSlotMapperId;
        this.guestCount = guestCount;
        this.confirmedAt = confirmedAt;
    }

    public String getBookingId() {
        return bookingId;
    }

    public void setBookingId(String bookingId) {
        this.bookingId = bookingId;
    }

    public Long getUserId() {
        return userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUserEmail() {
        return userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public Long getExperienceId() {
        return experienceId;
    }

    public void setExperienceId(Long experienceId) {
        this.experienceId = experienceId;
    }

    public Long getTimeSlotMapperId() {
        return timeSlotMapperId;
    }

    public void setTimeSlotMapperId(Long timeSlotMapperId) {
        this.timeSlotMapperId = timeSlotMapperId;
    }

    public Integer getGuestCount() {
        return guestCount;
    }

    public void setGuestCount(Integer guestCount) {
        this.guestCount = guestCount;
    }

    public LocalDateTime getConfirmedAt() {
        return confirmedAt;
    }

    public void setConfirmedAt(LocalDateTime confirmedAt) {
        this.confirmedAt = confirmedAt;
    }
}