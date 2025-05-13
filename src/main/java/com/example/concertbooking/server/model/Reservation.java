package com.example.concertbooking.server.model;

import com.example.concertbooking.grpc.ReserveRequest;

public class Reservation {

    private final String userId;
    private final String concertId;
    private final String seatTier;
    private final boolean afterParty;

    public Reservation(String userId, String concertId, String seatTier, boolean afterParty) {
        this.userId = userId;
        this.concertId = concertId;
        this.seatTier = seatTier;
        this.afterParty = afterParty;
    }

    public Reservation(ReserveRequest request) {
        this.userId = request.getUserId();
        this.concertId = request.getConcertId();
        this.seatTier = request.getSeatTier();
        this.afterParty = request.getIncludeAfterParty();
    }

    public String getUserId() {
        return userId;
    }

    public String getConcertId() {
        return concertId;
    }

    public String getSeatTier() {
        return seatTier;
    }

    public boolean isAfterParty() {
        return afterParty;
    }
}
