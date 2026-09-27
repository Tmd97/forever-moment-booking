package com.forvmom.MomentForeverBooking.mapper;

import com.forvmom.MomentForeverBooking.domain.entity.Booking;
import com.forvmom.MomentForeverBooking.domain.entity.BookingAddon;
import com.forvmom.MomentForeverBooking.domain.enums.BookingStatus;
import com.forvmom.MomentForeverBooking.domain.enums.PricingLevel;
import com.forvmom.MomentForeverBooking.events.BookingRequestEvent;
import com.forvmom.MomentForeverBooking.events.InboundEvent;

import java.time.LocalDateTime;

public class BookingMapper {

    public static Booking fromBookingRequestEvent(BookingRequestEvent event) {
        Booking booking = new Booking();
        booking.setBookingId(event.getBookingId());
        booking.setUserId(event.getUserId());
        booking.setUserEmail(event.getUserEmail());
        booking.setUserFullName(event.getUserFullName());
        booking.setExperienceId(event.getExperienceId());
        booking.setExperienceName(event.getExperienceName());
        booking.setLocationId(event.getLocationId());
        booking.setLocationName(event.getLocationName());
        booking.setTimeSlotMapperId(event.getTimeSlotMapperId());
        booking.setTimeSlotId(event.getTimeSlotId());
        booking.setTimeSlotLabel(event.getTimeSlotLabel());
        booking.setStartTime(event.getStartTime());
        booking.setEndTime(event.getEndTime());
        if (event.getBookingDate() != null) {
            booking.setBookingDate(event.getBookingDate().atStartOfDay());
        }
        booking.setGuestCount(event.getGuestCount());
        booking.setStatus(BookingStatus.PENDING);
        booking.setResolvedPricePerPerson(event.getResolvedPricePerPerson());
        booking.setPricingLevel(PricingLevel.valueOf(event.getPricingLevel()));
        booking.setTotalAmount(event.getTotalAmount());
        booking.setAddonsTotal(event.getAddonsTotal());
        booking.setGrandTotal(event.getGrandTotal());
        booking.setPincode(event.getPincode());
        booking.setRequestedAt(event.getRequestedAt() != null ? event.getRequestedAt() : LocalDateTime.now());

        if (event.getAddons() != null) {
            event.getAddons().forEach(addonSnap -> {
                BookingAddon addon = new BookingAddon();
                addon.setAddonMapperId(addonSnap.getAddonMapperId());
                addon.setAddonName(addonSnap.getAddonName());
                addon.setEffectivePrice(addonSnap.getEffectivePrice());
                addon.setFree(addonSnap.isFree());
                booking.addAddon(addon);
            });
        }
        return booking;
    }
}