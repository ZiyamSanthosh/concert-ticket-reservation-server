package com.example.concertbooking.server;

import com.example.concertbooking.grpc.*;
import com.example.concertbooking.server.etcd.EtcdHelper;
import com.example.concertbooking.server.model.Concert;
import com.example.concertbooking.server.model.Reservation;
import com.example.concertbooking.server.participant.AfterPartyParticipant;
import com.example.concertbooking.server.participant.ConcertParticipant;
import com.example.concertbooking.server.transaction.AtomicReservationCoordinator;
import com.example.concertbooking.server.zk.DistributedLock;
import com.example.concertbooking.server.zk.LeaderElection;
import com.example.concertbooking.server.zk.ZooKeeperClient;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;

public class ReservationServiceImpl extends ReservationServiceGrpc.ReservationServiceImplBase {

    private final Map<String, Concert> concertStore; // same as in ConcertServiceImpl
    private final Map<String, Reservation> reservations; // Key = userId + concertId
    private final ZooKeeperClient zkClient;
    private final int port;
    private final boolean isLeader;
    private final LeaderElection leaderElection;

//    public ReservationServiceImpl(Map<String, Concert> concertStore, ZooKeeperClient zkClient, int port, LeaderElection leaderElection) {
//        this.concertStore = concertStore;
//        this.zkClient = zkClient;
//        this.port = port;
//        this.isLeader = leaderElection.isLeader();
//        this.leaderElection = leaderElection;
//    }

    public ReservationServiceImpl(Map<String, Concert> concertStore,
                                  Map<String, Reservation> reservations,
                                  ZooKeeperClient zkClient,
                                  int port,
                                  LeaderElection leaderElection) {
        this.concertStore = concertStore;
        this.reservations = reservations;
        this.zkClient = zkClient;
        this.port = port;
        this.isLeader = leaderElection.isLeader();
        this.leaderElection = leaderElection;
    }

//    @Override
//    public void reserveTickets(ReserveRequest request, StreamObserver<ReservationResponse> responseObserver) {
//        String key = request.getUserId() + "-" + request.getConcertId();
//        if (reservations.containsKey(key)) {
//            respond(false, "Reservation already exists", responseObserver);
//            return;
//        }
//
//        Concert concert = concertStore.get(request.getConcertId());
//        if (concert == null) {
//            respond(false, "Concert not found", responseObserver);
//            return;
//        }
//
//        Concert.SeatTier tier = concert.getSeatTiers().get(request.getSeatTier());
//        if (tier == null) {
//            respond(false, "Invalid seat tier", responseObserver);
//            return;
//        }
//
//        synchronized (concert) {
//            if (tier.getAvailableSeats() <= 0) {
//                respond(false, "No seats available in this tier", responseObserver);
//                return;
//            }
//
//            if (request.getIncludeAfterParty() && concert.getAvailableAfterPartyTickets() <= 0) {
//                respond(false, "No after-party tickets available", responseObserver);
//                return;
//            }
//
//            // Update seat count
//            tier.setAvailableSeats(tier.getAvailableSeats() - 1);
//            if (request.getIncludeAfterParty()) {
//                concert.setAvailableAfterPartyTickets(concert.getAvailableAfterPartyTickets() - 1);
//            }
//
//            reservations.put(key, new Reservation(
//                    request.getUserId(),
//                    request.getConcertId(),
//                    request.getSeatTier(),
//                    request.getIncludeAfterParty()
//            ));
//        }
//
//        respond(true, "Reservation successful", responseObserver);
//    }

//    @Override
//    public void reserveTickets(ReserveRequest request, StreamObserver<ReservationResponse> responseObserver) {
//
//        if (!isLeader) {
//            ManagedChannel channel = null;
//            try {
//                String leaderAddress = leaderElection.getLeaderAddress();
//                if (leaderAddress == null) {
//                    respond(false, "❌ Leader unavailable. Try again.", responseObserver);
//                    return;
//                }
//
//                channel = ManagedChannelBuilder.forTarget(leaderAddress)
//                        .usePlaintext()
//                        .build();
//
//                ReservationServiceGrpc.ReservationServiceBlockingStub stub =
//                        ReservationServiceGrpc.newBlockingStub(channel);
//
//                ReservationResponse response = stub.reserveTickets(request);
//                channel.shutdown();
//
//                responseObserver.onNext(response);
//                responseObserver.onCompleted();
//                return;
//            } catch (Exception ex) {
//                ex.printStackTrace();
//                respond(false, "❌ Error forwarding to leader: " + ex.getMessage(), responseObserver);
//                return;
//            } finally {
//                if (channel != null) {
//                    channel.shutdown(); // 🔐 Always shut down the channel
//                }
//            }
//        }
//
//        System.out.println("📡 [Node " + port + "] Handling reserveTickets for ID: " + request.getConcertId());
//
//        try {
//            DistributedLock lock = new DistributedLock(zkClient, request.getConcertId());
//            lock.acquire();
//            try {
//                String key = request.getUserId() + "-" + request.getConcertId();
//                if (reservations.containsKey(key)) {
//                    respond(false, "Reservation already exists", responseObserver);
//                    return;
//                }
//
//                Concert concert = concertStore.get(request.getConcertId());
//                if (concert == null) {
//                    respond(false, "Concert not found", responseObserver);
//                    return;
//                }
//
//                Concert.SeatTier tier = concert.getSeatTiers().get(request.getSeatTier());
//                if (tier == null) {
//                    respond(false, "Invalid seat tier", responseObserver);
//                    return;
//                }
//
//                synchronized (concert) {
//                    if (tier.getAvailableSeats() <= 0) {
//                        respond(false, "No seats available in this tier", responseObserver);
//                        return;
//                    }
//
//                    if (request.getIncludeAfterParty() && concert.getAvailableAfterPartyTickets() <= 0) {
//                        respond(false, "No after-party tickets available", responseObserver);
//                        return;
//                    }
//
//                    // Update seat count
//                    tier.setAvailableSeats(tier.getAvailableSeats() - 1);
//                    if (request.getIncludeAfterParty()) {
//                        concert.setAvailableAfterPartyTickets(concert.getAvailableAfterPartyTickets() - 1);
//                    }
//
//                    reservations.put(key, new Reservation(
//                            request.getUserId(),
//                            request.getConcertId(),
//                            request.getSeatTier(),
//                            request.getIncludeAfterParty()
//                    ));
//                }
//
//                respond(true, "Reservation successful", responseObserver);
//
//                List<String> followers = EtcdHelper.getOtherNodes("http://localhost:2379", port);
//                for (String follower : followers) {
//                    try {
//                        ManagedChannel ch = ManagedChannelBuilder.forTarget(follower)
//                                .usePlaintext().build();
//                        ReservationServiceGrpc.ReservationServiceBlockingStub stub = ReservationServiceGrpc.newBlockingStub(ch);
//                        stub.syncReservation(request);
//                        ch.shutdown();
//                        System.out.println("📡 Synced reservation to: " + follower);
//                    } catch (Exception e) {
//                        System.err.println("⚠️ Reservation sync failed to: " + follower + " → " + e.getMessage());
//                    }
//                }
//
//            } finally {
//                lock.release();
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            respond(false, "ZooKeeper lock error: " + e.getMessage(), responseObserver);
//        }
//    }

    @Override
    public void reserveTickets(ReserveRequest request, StreamObserver<ReservationResponse> responseObserver) {

        if (!isLeader) {
            String leaderAddress = null;
            try {
                leaderAddress = leaderElection.getLeaderAddress();
                if (!isReachable(leaderAddress)) {
                    respond(false, "❌ Leader is not reachable. Try again later.", responseObserver);
                    return;
                }
            } catch (Exception e) {
                respond(false, "❌ Error fetching leader address: " + e.getMessage(), responseObserver);
                return;
            }

            if (leaderAddress == null) {
                respond(false, "❌ Leader unavailable. Try again.", responseObserver);
                return;
            }

            int maxRetries = 3;
            int attempt = 0;
            boolean forwarded = false;

            while (attempt < maxRetries && !forwarded) {
                ManagedChannel channel = null;
                try {
                    channel = ManagedChannelBuilder.forTarget(leaderAddress)
                            .usePlaintext()
                            .build();

                    ReservationServiceGrpc.ReservationServiceBlockingStub stub =
                            ReservationServiceGrpc.newBlockingStub(channel);

                    ReservationResponse response = stub.reserveTickets(request);
                    responseObserver.onNext(response);
                    responseObserver.onCompleted();
                    forwarded = true;
                } catch (Exception ex) {
                    System.err.println("⚠️ Attempt " + (attempt + 1) + " to forward to leader failed: " + ex.getMessage());
                    attempt++;
                    if (attempt == maxRetries) {
                        respond(false, "❌ Could not reach leader after " + maxRetries + " attempts.", responseObserver);
                    }
                } finally {
                    if (channel != null) {
                        channel.shutdown();
                        try {
                            if (!channel.awaitTermination(3, TimeUnit.SECONDS)) {
                                channel.shutdownNow();
                            }
                        } catch (InterruptedException e) {
                            channel.shutdownNow();
                        }
                    }
                }
            }
            return;
        }

        // ✅ Leader node handles reservation
        System.out.println("📡 [Node " + port + "] Handling reserveTickets for ID: " + request.getConcertId());

        try {
            DistributedLock lock = new DistributedLock(zkClient, request.getConcertId());
            lock.acquire();
            try {
                String key = request.getUserId() + "-" + request.getConcertId();
                if (reservations.containsKey(key)) {
                    respond(false, "Reservation already exists", responseObserver);
                    return;
                }

                Concert concert = concertStore.get(request.getConcertId());
                if (concert == null) {
                    respond(false, "Concert not found", responseObserver);
                    return;
                }

                Concert.SeatTier tier = concert.getSeatTiers().get(request.getSeatTier());
                if (tier == null) {
                    respond(false, "Invalid seat tier", responseObserver);
                    return;
                }

                synchronized (concert) {
                    if (tier.getAvailableSeats() <= 0) {
                        respond(false, "No seats available in this tier", responseObserver);
                        return;
                    }

                    if (request.getIncludeAfterParty() && concert.getAvailableAfterPartyTickets() <= 0) {
                        respond(false, "No after-party tickets available", responseObserver);
                        return;
                    }

                    tier.setAvailableSeats(tier.getAvailableSeats() - 1);
                    if (request.getIncludeAfterParty()) {
                        concert.setAvailableAfterPartyTickets(concert.getAvailableAfterPartyTickets() - 1);
                    }

                    reservations.put(key, new Reservation(
                            request.getUserId(),
                            request.getConcertId(),
                            request.getSeatTier(),
                            request.getIncludeAfterParty()
                    ));
                }

                respond(true, "Reservation successful", responseObserver);

                // 🔄 Sync to followers
                List<String> followers = EtcdHelper.getOtherNodes("http://localhost:2379", port);
                for (String follower : followers) {
                    ManagedChannel ch = null;
                    try {
                        ch = ManagedChannelBuilder.forTarget(follower)
                                .usePlaintext().build();
                        ReservationServiceGrpc.ReservationServiceBlockingStub stub = ReservationServiceGrpc.newBlockingStub(ch);
                        stub.syncReservation(request);
                        System.out.println("📡 Synced reservation to: " + follower);
                    } catch (Exception e) {
                        System.err.println("⚠️ Reservation sync failed to: " + follower + " → " + e.getMessage());
                    } finally {
                        if (ch != null) {
                            ch.shutdown();
                            try {
                                if (!ch.awaitTermination(3, TimeUnit.SECONDS)) {
                                    ch.shutdownNow();
                                }
                            } catch (InterruptedException e) {
                                ch.shutdownNow();
                            }
                        }
                    }
                }

            } finally {
                lock.release();
            }

        } catch (Exception e) {
            e.printStackTrace();
            respond(false, "ZooKeeper lock error: " + e.getMessage(), responseObserver);
        }
    }


//    @Override
//    public void cancelReservation(CancelReservationRequest request, StreamObserver<ReservationResponse> responseObserver) {
//        String key = request.getUserId() + "-" + request.getConcertId();
//        Reservation reservation = reservations.remove(key);
//
//        if (reservation == null) {
//            respond(false, "No such reservation found", responseObserver);
//            return;
//        }
//
//        Concert concert = concertStore.get(request.getConcertId());
//        if (concert != null) {
//            synchronized (concert) {
//                Concert.SeatTier tier = concert.getSeatTiers().get(reservation.getSeatTier());
//                if (tier != null) {
//                    tier.setAvailableSeats(tier.getAvailableSeats() + 1);
//                }
//                if (reservation.isAfterParty()) {
//                    concert.setAvailableAfterPartyTickets(concert.getAvailableAfterPartyTickets() + 1);
//                }
//            }
//        }
//
//        respond(true, "Reservation canceled", responseObserver);
//    }

    @Override
    public void cancelReservation(CancelReservationRequest request, StreamObserver<ReservationResponse> responseObserver) {

        if (!isLeader) {
            ManagedChannel channel = null;
            try {
                String leaderAddress = leaderElection.getLeaderAddress();
                if (leaderAddress == null) {
                    respond(false, "❌ Leader unavailable. Try again.", responseObserver);
                    return;
                }

                channel = ManagedChannelBuilder.forTarget(leaderAddress)
                        .usePlaintext()
                        .build();

                ReservationServiceGrpc.ReservationServiceBlockingStub stub =
                        ReservationServiceGrpc.newBlockingStub(channel);

                ReservationResponse response = stub.cancelReservation(request);
                channel.shutdown();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;
            } catch (Exception ex) {
                ex.printStackTrace();
                respond(false, "❌ Error forwarding to leader: " + ex.getMessage(), responseObserver);
                return;
            } finally {
                if (channel != null) {
                    channel.shutdown(); // 🔐 Always shut down the channel
                }
            }
        }

        System.out.println("📡 [Node " + port + "] Handling cancelReservation for ID: " + request.getConcertId());

        try {
            DistributedLock lock = new DistributedLock(zkClient, request.getConcertId());
            lock.acquire();
            try {
                String key = request.getUserId() + "-" + request.getConcertId();
                Reservation reservation = reservations.remove(key);

                if (reservation == null) {
                    respond(false, "No such reservation found", responseObserver);
                    return;
                }

                Concert concert = concertStore.get(request.getConcertId());
                if (concert != null) {
                    synchronized (concert) {
                        Concert.SeatTier tier = concert.getSeatTiers().get(reservation.getSeatTier());
                        if (tier != null) {
                            tier.setAvailableSeats(tier.getAvailableSeats() + 1);
                        }
                        if (reservation.isAfterParty()) {
                            concert.setAvailableAfterPartyTickets(concert.getAvailableAfterPartyTickets() + 1);
                        }
                    }
                }

                respond(true, "Reservation canceled", responseObserver);

                List<String> followers = EtcdHelper.getOtherNodes("http://localhost:2379", port);
                for (String follower : followers) {
                    try {
                        ManagedChannel ch = ManagedChannelBuilder.forTarget(follower)
                                .usePlaintext().build();
                        ReservationServiceGrpc.ReservationServiceBlockingStub stub = ReservationServiceGrpc.newBlockingStub(ch);
                        stub.syncCancelReservation(request);
                        ch.shutdown();
                        System.out.println("📡 Synced cancel to: " + follower);
                    } catch (Exception e) {
                        System.err.println("⚠️ Cancel sync failed to: " + follower + " → " + e.getMessage());
                    }
                }

            } finally {
                lock.release();
            }
        } catch (Exception e) {
            e.printStackTrace();
            respond(false, "ZooKeeper lock error: " + e.getMessage(), responseObserver);
        }
    }


    @Override
    public void getAvailability(AvailabilityRequest request, StreamObserver<AvailabilityResponse> responseObserver) {

        System.out.println("📡 [Node " + port + "] Handling getAvailability for ID: " + request.getConcertId());

        Concert concert = concertStore.get(request.getConcertId());
        if (concert == null) {
            responseObserver.onNext(AvailabilityResponse.newBuilder().build());
            responseObserver.onCompleted();
            return;
        }

        AvailabilityResponse.Builder builder = AvailabilityResponse.newBuilder();
        for (Concert.SeatTier tier : concert.getSeatTiers().values()) {
            builder.addSeatTiers(TierAvailability.newBuilder()
                    .setTierName(tier.getTierName())
                    .setAvailable(tier.getAvailableSeats())
                    .build());
        }

        builder.setAfterPartyTicketsAvailable(concert.getAvailableAfterPartyTickets());

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

//    @Override
//    public void reserveWithAfterParty(ReserveRequest request, StreamObserver<ReservationResponse> responseObserver) {
//
//        if (!isLeader) {
//            ManagedChannel channel = null;
//            try {
//                String leaderAddress = leaderElection.getLeaderAddress();
//                if (leaderAddress == null) {
//                    respond(false, "❌ Leader unavailable. Try again.", responseObserver);
//                    return;
//                }
//
//                channel = ManagedChannelBuilder.forTarget(leaderAddress)
//                        .usePlaintext()
//                        .build();
//
//                ReservationServiceGrpc.ReservationServiceBlockingStub stub =
//                        ReservationServiceGrpc.newBlockingStub(channel);
//
//                ReservationResponse response = stub.reserveWithAfterParty(request);
//                channel.shutdown();
//
//                responseObserver.onNext(response);
//                responseObserver.onCompleted();
//                return;
//
//            } catch (Exception ex) {
//                ex.printStackTrace();
//                respond(false, "❌ Error forwarding to leader: " + ex.getMessage(), responseObserver);
//                return;
//            } finally {
//                if (channel != null) {
//                    channel.shutdown(); // 🔐 Always shut down the channel
//                }
//            }
//        }
//
//        System.out.println("📡 [Node " + port + "] Handling reserveWithAfterParty for ID: " + request.getConcertId());
//
//        String userId = request.getUserId();
//        String concertId = request.getConcertId();
//        String seatTier = request.getSeatTier();
//        int numberOfTickets = request.getNumberOfTickets();  // ✅ NEW
//
//        try {
//            ConcertParticipant concertParticipant = new ConcertParticipant(concertStore);
//            AfterPartyParticipant afterPartyParticipant = new AfterPartyParticipant(concertStore);
//
//            AtomicReservationCoordinator coordinator = new AtomicReservationCoordinator(
//                    concertParticipant,
//                    afterPartyParticipant
//            );
//
//            boolean success = coordinator.reserve(userId, concertId, seatTier, numberOfTickets);
//            if (success) {
//                List<String> followers = EtcdHelper.getOtherNodes("http://localhost:2379", port);
//                for (String follower : followers) {
//                    try {
//                        ManagedChannel ch = ManagedChannelBuilder.forTarget(follower)
//                                .usePlaintext().build();
//                        ReservationServiceGrpc.ReservationServiceBlockingStub stub =
//                                ReservationServiceGrpc.newBlockingStub(ch);
//                        stub.syncReservation(request);  // same message type
//                        ch.shutdown();
//                        System.out.println("📡 Synced 2PC reservation to: " + follower);
//                    } catch (Exception e) {
//                        System.err.println("⚠️ Failed to sync 2PC reservation to " + follower + ": " + e.getMessage());
//                    }
//                }
//                respond(true, "2PC Reservation successful", responseObserver);
//            } else {
//                respond(false, "2PC Reservation failed (rolled back)", responseObserver);
//            }
//        } catch (Exception e) {
//            e.printStackTrace();
//            respond(false, "2PC Reservation error: " + e.getMessage(), responseObserver);
//        }
//    }

    @Override
    public void reserveWithAfterParty(ReserveRequest request, StreamObserver<ReservationResponse> responseObserver) {

        if (!isLeader) {
            ManagedChannel channel = null;
            try {
                String leaderAddress = leaderElection.getLeaderAddress();
                if (leaderAddress == null) {
                    respond(false, "❌ Leader unavailable. Try again.", responseObserver);
                    return;
                }

                channel = ManagedChannelBuilder.forTarget(leaderAddress)
                        .usePlaintext()
                        .build();

                ReservationServiceGrpc.ReservationServiceBlockingStub stub =
                        ReservationServiceGrpc.newBlockingStub(channel);

                ReservationResponse response = stub.reserveWithAfterParty(request);
                responseObserver.onNext(response);
                responseObserver.onCompleted();

            } catch (Exception ex) {
                ex.printStackTrace();
                respond(false, "❌ Error forwarding to leader: " + ex.getMessage(), responseObserver);
            } finally {
                if (channel != null) {
                    channel.shutdown(); // 🔐 Always shut down the channel
                }
            }
            return;
        }

        System.out.println("📡 [Node " + port + "] Handling reserveWithAfterParty for ID: " + request.getConcertId());

        String userId = request.getUserId();
        String concertId = request.getConcertId();
        String seatTier = request.getSeatTier();
        int numberOfTickets = request.getNumberOfTickets();  // ✅ NEW

        try {
            ConcertParticipant concertParticipant = new ConcertParticipant(concertStore);
            AfterPartyParticipant afterPartyParticipant = new AfterPartyParticipant(concertStore);

            AtomicReservationCoordinator coordinator = new AtomicReservationCoordinator(
                    concertParticipant,
                    afterPartyParticipant
            );

            boolean success = coordinator.reserve(userId, concertId, seatTier, numberOfTickets);
            if (success) {
                List<String> followers = EtcdHelper.getOtherNodes("http://localhost:2379", port);
                for (String follower : followers) {
                    try {
                        ManagedChannel ch = ManagedChannelBuilder.forTarget(follower)
                                .usePlaintext().build();
                        ReservationServiceGrpc.ReservationServiceBlockingStub stub =
                                ReservationServiceGrpc.newBlockingStub(ch);

                        // 🔁 Sync each individual reservation
                        for (int i = 0; i < numberOfTickets; i++) {
                            String idSuffix = "#" + i;
                            ReserveRequest individualRequest = ReserveRequest.newBuilder()
                                    .setConcertId(concertId)
                                    .setUserId(userId + idSuffix)
                                    .setSeatTier(seatTier)
                                    .setIncludeAfterParty(true)
                                    .build();

                            stub.syncReservation(individualRequest);
                        }

                        ch.shutdown();
                        System.out.println("📡 Synced 2PC reservation to: " + follower);
                    } catch (Exception e) {
                        System.err.println("⚠️ Failed to sync 2PC reservation to " + follower + ": " + e.getMessage());
                    }
                }

                respond(true, "2PC Reservation successful", responseObserver);
            } else {
                respond(false, "2PC Reservation failed (rolled back)", responseObserver);
            }
        } catch (Exception e) {
            e.printStackTrace();
            respond(false, "2PC Reservation error: " + e.getMessage(), responseObserver);
        }
    }


    @Override
    public void syncReservation(ReserveRequest request, StreamObserver<ReservationResponse> responseObserver) {
        String key = request.getUserId() + "-" + request.getConcertId();

        if (reservations.containsKey(key)) {
            respond(false, "Reservation already exists", responseObserver);
            return;
        }

        Concert concert = concertStore.get(request.getConcertId());
        if (concert == null) {
            respond(false, "Concert not found", responseObserver);
            return;
        }

        Concert.SeatTier tier = concert.getSeatTiers().get(request.getSeatTier());
        if (tier == null) {
            respond(false, "Invalid seat tier", responseObserver);
            return;
        }

        synchronized (concert) {
            tier.setAvailableSeats(tier.getAvailableSeats() - 1);
            if (request.getIncludeAfterParty()) {
                concert.setAvailableAfterPartyTickets(concert.getAvailableAfterPartyTickets() - 1);
            }

            reservations.put(key, new Reservation(
                    request.getUserId(),
                    request.getConcertId(),
                    request.getSeatTier(),
                    request.getIncludeAfterParty()
            ));
        }

        System.out.println("🎯 Synced reservation: " + key);
        respond(true, "Reservation synced", responseObserver);
    }

    @Override
    public void syncCancelReservation(CancelReservationRequest request, StreamObserver<ReservationResponse> responseObserver) {
        String key = request.getUserId() + "-" + request.getConcertId();
        Reservation reservation = reservations.remove(key);

        if (reservation == null) {
            respond(false, "No such reservation found", responseObserver);
            return;
        }

        Concert concert = concertStore.get(request.getConcertId());
        if (concert != null) {
            synchronized (concert) {
                Concert.SeatTier tier = concert.getSeatTiers().get(reservation.getSeatTier());
                if (tier != null) {
                    tier.setAvailableSeats(tier.getAvailableSeats() + 1);
                }
                if (reservation.isAfterParty()) {
                    concert.setAvailableAfterPartyTickets(concert.getAvailableAfterPartyTickets() + 1);
                }
            }
        }

        System.out.println("🗑️ Synced cancellation: " + key);
        respond(true, "Cancel synced", responseObserver);
    }

    @Override
    public void fetchAllReservations(Empty request, StreamObserver<AllReservationsResponse> responseObserver) {
        AllReservationsResponse.Builder builder = AllReservationsResponse.newBuilder();
        for (Reservation r : reservations.values()) {
            builder.addReservations(ReserveRequest.newBuilder()
                    .setUserId(r.getUserId())
                    .setConcertId(r.getConcertId())
                    .setSeatTier(r.getSeatTier())
                    .setIncludeAfterParty(r.isAfterParty()));
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }


    private void respond(boolean success, String message, StreamObserver<ReservationResponse> observer) {
        observer.onNext(ReservationResponse.newBuilder()
                .setSuccess(success)
                .setMessage(message)
                .build());
        observer.onCompleted();
    }

    private boolean isReachable(String address) {
        try {
            String[] parts = address.split(":");
            String host = parts[0];
            int port = Integer.parseInt(parts[1]);
            try (Socket socket = new Socket()) {
                socket.connect(new InetSocketAddress(host, port), 500); // 500ms timeout
                return true;
            }
        } catch (Exception e) {
            return false;
        }
    }

}
