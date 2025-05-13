package com.example.concertbooking.server.model;

import com.example.concertbooking.grpc.AddConcertRequest;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class Concert {

    private String concertId;
    private String concertName;
    private Map<String, SeatTier> seatTiers = new HashMap<>();
    private int totalAfterPartyTickets;
    private int availableAfterPartyTickets;

    public Concert(String concertId, String concertName, int afterPartyTickets) {
        this.concertId = concertId;
        this.concertName = concertName;
        this.totalAfterPartyTickets = afterPartyTickets;
        this.availableAfterPartyTickets = afterPartyTickets;
    }

    public Concert(AddConcertRequest request) {
        this.concertId = request.getConcertId();
        this.concertName = request.getConcertName();
        this.totalAfterPartyTickets = request.getAfterPartyTickets();
        this.availableAfterPartyTickets = request.getAfterPartyTickets();

        this.seatTiers = new ConcurrentHashMap<>();
        for (com.example.concertbooking.grpc.SeatTier tier : request.getSeatTiersList()) {
            this.seatTiers.put(tier.getTierName(), new SeatTier(
                    tier.getTierName(),
                    tier.getTotalSeats(),
                    tier.getPrice()
            ));
        }
    }

    public String getConcertId() {
        return concertId;
    }

    public String getConcertName() {
        return concertName;
    }

    public Map<String, SeatTier> getSeatTiers() {
        return seatTiers;
    }

    public int getAvailableAfterPartyTickets() {
        return availableAfterPartyTickets;
    }

    public void setAvailableAfterPartyTickets(int availableAfterPartyTickets) {
        this.availableAfterPartyTickets = availableAfterPartyTickets;
    }

    public void addOrUpdateTier(String name, int totalSeats, double price) {
        seatTiers.put(name, new SeatTier(name, totalSeats, price));
    }

    public void setConcertName(String concertName) {
        this.concertName = concertName;
    }

    public static class SeatTier {
        private String tierName;
        private int totalSeats;
        private int availableSeats;
        private double price;

        public SeatTier(String tierName, int totalSeats, double price) {
            this.tierName = tierName;
            this.totalSeats = totalSeats;
            this.availableSeats = totalSeats;
            this.price = price;
        }

        public String getTierName() {
            return tierName;
        }

        public int getAvailableSeats() {
            return availableSeats;
        }

        public void setAvailableSeats(int availableSeats) {
            this.availableSeats = availableSeats;
        }

        public int getTotalSeats() {
            return totalSeats;
        }

        public double getPrice() {
            return price;
        }

        public void setPrice(double price) {
            this.price = price;
        }
    }
}
