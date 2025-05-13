package com.example.concertbooking.server.participant;

import com.example.concertbooking.server.model.Concert;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class AfterPartyParticipant {

    private final Map<String, Concert> concertStore;
    private final Map<String, Boolean> prepared = new ConcurrentHashMap<>();
    // Key: userId + concertId

    public AfterPartyParticipant(Map<String, Concert> concertStore) {
        this.concertStore = concertStore;
    }

    public boolean prepare(String userId, String concertId) {
        Concert concert = concertStore.get(concertId);
        if (concert == null) return false;

        synchronized (concert) {
            if (concert.getAvailableAfterPartyTickets() <= 0) {
                return false;
            }

            concert.setAvailableAfterPartyTickets(concert.getAvailableAfterPartyTickets() - 1);
            prepared.put(userId + "-" + concertId, true);
            return true;
        }
    }

    public void commit(String userId, String concertId) {
        prepared.remove(userId + "-" + concertId);
    }

    public void abort(String userId, String concertId) {
        Concert concert = concertStore.get(concertId);
        if (concert == null) return;

        synchronized (concert) {
            concert.setAvailableAfterPartyTickets(concert.getAvailableAfterPartyTickets() + 1);
        }

        prepared.remove(userId + "-" + concertId);
    }
}
