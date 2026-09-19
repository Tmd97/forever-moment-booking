package com.forvmom.MomentForeverBooking.events;

import org.springframework.stereotype.Component;

import java.math.BigDecimal;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;

public class BookingRequestEvent extends BaseEvent implements InboundEvent {
    private String bookingId;
    private Long userId;
    private String userEmail;
    private String userFullName;
    private Long experienceId;
    private String experienceName;
    private String experienceSlug;
    private Long locationId;
    private String locationName;
    private Long timeSlotMapperId;
    private Long timeSlotId;
    private String timeSlotLabel;
    private String startTime;
    private String endTime;
    private LocalDate bookingDate;
    private Integer guestCount;
    private String pincode;
    private BigDecimal resolvedPricePerPerson;
    private String pricingLevel;
    private BigDecimal totalAmount;
    private Integer availableCapacity;
    private List<BookedAddonSnapshot> addons;
    private BigDecimal addonsTotal;
    private BigDecimal grandTotal;
    private LocalDateTime requestedAt;

    public BookingRequestEvent() {
    }

    public String getBookingId() {
        return this.bookingId;
    }

    public void setBookingId(String bookingId) {
        this.bookingId = bookingId;
    }

    public Long getUserId() {
        return this.userId;
    }

    public void setUserId(Long userId) {
        this.userId = userId;
    }

    public String getUserEmail() {
        return this.userEmail;
    }

    public void setUserEmail(String userEmail) {
        this.userEmail = userEmail;
    }

    public String getUserFullName() {
        return this.userFullName;
    }

    public void setUserFullName(String userFullName) {
        this.userFullName = userFullName;
    }

    public Long getExperienceId() {
        return this.experienceId;
    }

    public void setExperienceId(Long experienceId) {
        this.experienceId = experienceId;
    }

    public String getExperienceName() {
        return this.experienceName;
    }

    public void setExperienceName(String experienceName) {
        this.experienceName = experienceName;
    }

    public String getExperienceSlug() {
        return this.experienceSlug;
    }

    public void setExperienceSlug(String experienceSlug) {
        this.experienceSlug = experienceSlug;
    }

    public Long getLocationId() {
        return this.locationId;
    }

    public void setLocationId(Long locationId) {
        this.locationId = locationId;
    }

    public String getLocationName() {
        return this.locationName;
    }

    public void setLocationName(String locationName) {
        this.locationName = locationName;
    }

    public Long getTimeSlotMapperId() {
        return this.timeSlotMapperId;
    }

    public void setTimeSlotMapperId(Long timeSlotMapperId) {
        this.timeSlotMapperId = timeSlotMapperId;
    }

    public Long getTimeSlotId() {
        return this.timeSlotId;
    }

    public void setTimeSlotId(Long timeSlotId) {
        this.timeSlotId = timeSlotId;
    }

    public String getTimeSlotLabel() {
        return this.timeSlotLabel;
    }

    public void setTimeSlotLabel(String timeSlotLabel) {
        this.timeSlotLabel = timeSlotLabel;
    }

    public String getStartTime() {
        return this.startTime;
    }

    public void setStartTime(String startTime) {
        this.startTime = startTime;
    }

    public String getEndTime() {
        return this.endTime;
    }

    public void setEndTime(String endTime) {
        this.endTime = endTime;
    }

    public LocalDate getBookingDate() {
        return this.bookingDate;
    }

    public void setBookingDate(LocalDate bookingDate) {
        this.bookingDate = bookingDate;
    }

    public Integer getGuestCount() {
        return this.guestCount;
    }

    public void setGuestCount(Integer guestCount) {
        this.guestCount = guestCount;
    }

    public String getPincode() {
        return this.pincode;
    }

    public void setPincode(String pincode) {
        this.pincode = pincode;
    }

    public BigDecimal getResolvedPricePerPerson() {
        return this.resolvedPricePerPerson;
    }

    public void setResolvedPricePerPerson(BigDecimal resolvedPricePerPerson) {
        this.resolvedPricePerPerson = resolvedPricePerPerson;
    }

    public String getPricingLevel() {
        return this.pricingLevel;
    }

    public void setPricingLevel(String pricingLevel) {
        this.pricingLevel = pricingLevel;
    }

    public BigDecimal getTotalAmount() {
        return this.totalAmount;
    }

    public void setTotalAmount(BigDecimal totalAmount) {
        this.totalAmount = totalAmount;
    }

    public Integer getAvailableCapacity() {
        return this.availableCapacity;
    }

    public void setAvailableCapacity(Integer availableCapacity) {
        this.availableCapacity = availableCapacity;
    }

    public List<BookedAddonSnapshot> getAddons() {
        return this.addons;
    }

    public void setAddons(List<BookedAddonSnapshot> addons) {
        this.addons = addons;
    }

    public BigDecimal getAddonsTotal() {
        return this.addonsTotal;
    }

    public void setAddonsTotal(BigDecimal addonsTotal) {
        this.addonsTotal = addonsTotal;
    }

    public BigDecimal getGrandTotal() {
        return this.grandTotal;
    }

    public void setGrandTotal(BigDecimal grandTotal) {
        this.grandTotal = grandTotal;
    }

    public LocalDateTime getRequestedAt() {
        return this.requestedAt;
    }

    public void setRequestedAt(LocalDateTime requestedAt) {
        this.requestedAt = requestedAt;
    }

    public static class BookedAddonSnapshot {
        private Long addonMapperId;
        private String addonName;
        private BigDecimal effectivePrice;
        private boolean free;

        public BookedAddonSnapshot() {
        }

        public BookedAddonSnapshot(Long addonMapperId, String addonName, BigDecimal effectivePrice, boolean free) {
            this.addonMapperId = addonMapperId;
            this.addonName = addonName;
            this.effectivePrice = effectivePrice;
            this.free = free;
        }

        public Long getAddonMapperId() {
            return this.addonMapperId;
        }

        public void setAddonMapperId(Long addonMapperId) {
            this.addonMapperId = addonMapperId;
        }

        public String getAddonName() {
            return this.addonName;
        }

        public void setAddonName(String addonName) {
            this.addonName = addonName;
        }

        public BigDecimal getEffectivePrice() {
            return this.effectivePrice;
        }

        public void setEffectivePrice(BigDecimal effectivePrice) {
            this.effectivePrice = effectivePrice;
        }

        public boolean isFree() {
            return this.free;
        }

        public void setFree(boolean free) {
            this.free = free;
        }


    }
}