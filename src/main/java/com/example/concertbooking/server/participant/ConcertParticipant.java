package com.example.concertbooking.server.participant;

import com.example.concertbooking.server.model.Concert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConcertParticipant {

    private final Map<String, Concert> concertStore;
    private final Map<String, String> preparedSeats = new ConcurrentHashMap<>();
    // Key: userId + concertId, Value: seatTier

    public ConcertParticipant(Map<String, Concert> concertStore) {
        this.concertStore = concertStore;
    }

    public boolean prepare(String userId, String concertId, String seatTier) {
        Concert concert = concertStore.get(concertId);
        if (concert == null) return false;

        synchronized (concert) {
            Concert.SeatTier tier = concert.getSeatTiers().get(seatTier);
            if (tier == null || tier.getAvailableSeats() <= 0) {
                return false;
            }

            // Temporarily mark the seat
            tier.setAvailableSeats(tier.getAvailableSeats() - 1);
            preparedSeats.put(userId + "-" + concertId, seatTier);
            return true;
        }
    }

    public void commit(String userId, String concertId, String seatTier) {
        // Nothing extra to do: seat was already decremented in prepare
        preparedSeats.remove(userId + "-" + concertId);
    }

    public void abort(String userId, String concertId, String seatTier) {
        Concert concert = concertStore.get(concertId);
        if (concert == null) return;

        synchronized (concert) {
            Concert.SeatTier tier = concert.getSeatTiers().get(seatTier);
            if (tier != null) {
                tier.setAvailableSeats(tier.getAvailableSeats() + 1); // undo
            }
        }

        preparedSeats.remove(userId + "-" + concertId);
    }
}
