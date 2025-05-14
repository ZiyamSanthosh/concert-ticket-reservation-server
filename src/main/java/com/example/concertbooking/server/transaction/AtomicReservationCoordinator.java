package com.example.concertbooking.server.transaction;

import com.example.concertbooking.server.participant.AfterPartyParticipant;
import com.example.concertbooking.server.participant.ConcertParticipant;

public class AtomicReservationCoordinator {

    private final ConcertParticipant concertParticipant;
    private final AfterPartyParticipant afterPartyParticipant;

    public AtomicReservationCoordinator(ConcertParticipant concertParticipant, AfterPartyParticipant afterPartyParticipant) {
        this.concertParticipant = concertParticipant;
        this.afterPartyParticipant = afterPartyParticipant;
    }

    // When combo atomic reservation is needed.
    public boolean reserve(String userId, String concertId, String seatTier, int count) {
        boolean[] concertPrepared = new boolean[count];
        boolean[] afterPartyPrepared = new boolean[count];

        try {
            // Phase 1: Prepare all
            for (int i = 0; i < count; i++) {
                String idSuffix = "#" + i;
                concertPrepared[i] = concertParticipant.prepare(userId + idSuffix, concertId, seatTier);
                afterPartyPrepared[i] = afterPartyParticipant.prepare(userId + idSuffix, concertId);

                if (!concertPrepared[i] || !afterPartyPrepared[i]) {
                    System.out.println("Preparation failed at index " + i);
                    throw new RuntimeException("Prepare failed for one of the reservations");
                }
            }

            // Phase 2: Commit all
            for (int i = 0; i < count; i++) {
                String idSuffix = "#" + i;
                concertParticipant.commit(userId + idSuffix, concertId, seatTier);
                afterPartyParticipant.commit(userId + idSuffix, concertId);
            }

            return true;

        } catch (Exception e) {
            System.out.println("2PC failure: " + e.getMessage());

            // Rollback everything that was prepared
            for (int i = 0; i < count; i++) {
                String idSuffix = "#" + i;
                if (concertPrepared[i]) {
                    concertParticipant.abort(userId + idSuffix, concertId, seatTier);
                }
                if (afterPartyPrepared[i]) {
                    afterPartyParticipant.abort(userId + idSuffix, concertId);
                }
            }
        }

        return false;
    }
}
