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

//    public boolean reserve(String userId, String concertId, String seatTier) {
//        boolean concertPrepared = false;
//        boolean afterPartyPrepared = false;
//
//        try {
//            concertPrepared = concertParticipant.prepare(userId, concertId, seatTier);
//            afterPartyPrepared = afterPartyParticipant.prepare(userId, concertId);
//        } catch (Exception e) {
//            System.out.println("⚠️ Prepare phase failed: " + e.getMessage());
//        }
//
//        if (concertPrepared && afterPartyPrepared) {
//            try {
//                concertParticipant.commit(userId, concertId, seatTier);
//                afterPartyParticipant.commit(userId, concertId);
//                return true;
//            } catch (Exception e) {
//                System.out.println("⚠️ Commit failed, but already prepared: " + e.getMessage());
//                // optional: rollback strategy
//            }
//        } else {
//            // Abort both if either failed
//            if (concertPrepared) {
//                concertParticipant.abort(userId, concertId, seatTier);
//            }
//            if (afterPartyPrepared) {
//                afterPartyParticipant.abort(userId, concertId);
//            }
//        }
//
//        return false;
//    }

    // When combo atomic reservation is not needed.
//    public boolean reserve(String userId, String concertId, String seatTier) {
//        boolean concertPrepared = false;
//        boolean afterPartyPrepared = false;
//
//        try {
//            // Phase 1: Prepare
//            concertPrepared = concertParticipant.prepare(userId, concertId, seatTier);
//            afterPartyPrepared = afterPartyParticipant.prepare(userId, concertId);
//
//            // Simulate crash between prepare and commit. Only for demo purposes.
//            if (Math.random() < 0.5) {
//                System.out.println("❌ Simulated coordinator crash after prepare!");
//                throw new RuntimeException("Simulated crash after prepare");
//            }
//
//            // Phase 2: Commit (if both said yes)
//            if (concertPrepared && afterPartyPrepared) {
//                concertParticipant.commit(userId, concertId, seatTier);
//                afterPartyParticipant.commit(userId, concertId);
//                return true;
//            }
//
//        } catch (Exception e) {
//            System.out.println("⚠️ 2PC failure: " + e.getMessage());
//        }
//
//        // Rollback if needed
//        if (concertPrepared) {
//            concertParticipant.abort(userId, concertId, seatTier);
//        }
//        if (afterPartyPrepared) {
//            afterPartyParticipant.abort(userId, concertId);
//        }
//
//        return false;
//    }


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
                    System.out.println("❌ Preparation failed at index " + i);
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
            System.out.println("⚠️ 2PC failure: " + e.getMessage());

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
