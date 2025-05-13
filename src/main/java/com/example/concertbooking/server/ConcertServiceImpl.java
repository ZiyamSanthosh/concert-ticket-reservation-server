package com.example.concertbooking.server;

import com.example.concertbooking.grpc.*;
import com.example.concertbooking.grpc.TicketReservationProto.*;
import com.example.concertbooking.server.etcd.EtcdHelper;
import com.example.concertbooking.server.model.Concert;

import com.example.concertbooking.server.zk.DistributedLock;
import com.example.concertbooking.server.zk.LeaderElection;
import com.example.concertbooking.server.zk.ZooKeeperClient;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.stub.StreamObserver;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConcertServiceImpl extends ConcertServiceGrpc.ConcertServiceImplBase {

    // In-memory store
    private final Map<String, Concert> concertStore;
    private final ZooKeeperClient zkClient;
    private final LeaderElection leaderElection;
    private final int port;
    private final boolean isLeader;

    public ConcertServiceImpl(Map<String, Concert> concertStore, ZooKeeperClient zkClient, int port, LeaderElection leaderElection) {
        this.concertStore = concertStore;
        this.zkClient = zkClient;
        this.port = port;
        this.leaderElection = leaderElection;
        this.isLeader = leaderElection.isLeader();
    }

//    @Override
//    public void addConcert(AddConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {
//        String concertId = request.getConcertId();
//
//        if (concertStore.containsKey(concertId)) {
//            responseObserver.onNext(ConcertResponse.newBuilder()
//                    .setSuccess(false)
//                    .setMessage("Concert already exists")
//                    .build());
//            responseObserver.onCompleted();
//            return;
//        }
//
//        Concert concert = new Concert(concertId, request.getConcertName(), request.getAfterPartyTickets());
//        for (SeatTier tier : request.getSeatTiersList()) {
//            concert.addOrUpdateTier(tier.getTierName(), tier.getTotalSeats(), tier.getPrice());
//        }
//
//        concertStore.put(concertId, concert);
//
//        responseObserver.onNext(ConcertResponse.newBuilder()
//                .setSuccess(true)
//                .setMessage("Concert added successfully")
//                .build());
//        responseObserver.onCompleted();
//    }

    @Override
    public void addConcert(AddConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {

        if (!isLeader) {
            try {
                // Forward to current leader
                String leaderAddress = leaderElection.getLeaderAddress();
                if (leaderAddress == null) {
                    respond(false, "❌ Leader unavailable. Try again.", responseObserver);
                    return;
                }

                System.out.println("📡 [Node " + port + "] Forwarding addConcert to leader: " + leaderAddress);

                ManagedChannel channel = ManagedChannelBuilder.forTarget(leaderAddress)
                        .usePlaintext()
                        .build();

                ConcertServiceGrpc.ConcertServiceBlockingStub stub =
                        ConcertServiceGrpc.newBlockingStub(channel);

                ConcertResponse response = stub.addConcert(request);
                channel.shutdown();

                // Send back leader's response
                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;

            } catch (Exception ex) {
                ex.printStackTrace();
                respond(false, "❌ Error forwarding to leader: " + ex.getMessage(), responseObserver);
                return;
            }
        }

        String concertId = request.getConcertId();
        System.out.println("📡 [Node " + port + "] Handling addConcert for ID: " + concertId);

        try {
            DistributedLock lock = new DistributedLock(zkClient, concertId);
            try {
                lock.acquire();

                if (concertStore.containsKey(concertId)) {
                    respond(false, "Concert already exists", responseObserver);
                    return;
                }

                Concert concert = new Concert(concertId, request.getConcertName(), request.getAfterPartyTickets());
                for (SeatTier tier : request.getSeatTiersList()) {
                    concert.addOrUpdateTier(tier.getTierName(), tier.getTotalSeats(), tier.getPrice());
                }

                concertStore.put(concertId, concert);
                respond(true, "Concert added successfully", responseObserver);

                List<String> followers = EtcdHelper.getOtherNodes("http://localhost:2379", port);

                for (String followerAddress : followers) {
                    try {
                        ManagedChannel channel = ManagedChannelBuilder.forTarget(followerAddress)
                                .usePlaintext()
                                .build();

                        ConcertServiceGrpc.ConcertServiceBlockingStub stub =
                                ConcertServiceGrpc.newBlockingStub(channel);

                        stub.syncConcert(request);
                        channel.shutdown();

                        System.out.println("📡 Synced concert with follower: " + followerAddress);
                    } catch (Exception e) {
                        System.err.println("⚠️ Failed to sync with follower: " + followerAddress + " - " + e.getMessage());
                    }
                }


            } catch (Exception e) {
                e.printStackTrace();
                respond(false, "ZooKeeper lock error: " + e.getMessage(), responseObserver);
            } finally {
                lock.release();
            }
        } catch (Exception e) {
            e.printStackTrace();
            respond(false, "ZooKeeper lock error: " + e.getMessage(), responseObserver);
        }
    }


//    @Override
//    public void updateConcert(UpdateConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {
//        Concert concert = concertStore.get(request.getConcertId());
//
//        if (concert == null) {
//            responseObserver.onNext(ConcertResponse.newBuilder()
//                    .setSuccess(false)
//                    .setMessage("Concert not found")
//                    .build());
//            responseObserver.onCompleted();
//            return;
//        }
//
//        for (SeatTier tier : request.getSeatTiersList()) {
//            concert.addOrUpdateTier(tier.getTierName(), tier.getTotalSeats(), tier.getPrice());
//        }
//
//        concert.setAvailableAfterPartyTickets(request.getAfterPartyTickets());
//
//        responseObserver.onNext(ConcertResponse.newBuilder()
//                .setSuccess(true)
//                .setMessage("Concert updated successfully")
//                .build());
//        responseObserver.onCompleted();
//    }

    @Override
    public void updateConcert(UpdateConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {

        if (!isLeader) {
            try {
                String leaderAddress = leaderElection.getLeaderAddress();
                if (leaderAddress == null) {
                    respond(false, "❌ Leader unavailable. Try again.", responseObserver);
                    return;
                }

                System.out.println("📡 [Node " + port + "] Forwarding addConcert to leader: " + leaderAddress);

                ManagedChannel channel = ManagedChannelBuilder.forTarget(leaderAddress)
                        .usePlaintext()
                        .build();

                ConcertServiceGrpc.ConcertServiceBlockingStub stub =
                        ConcertServiceGrpc.newBlockingStub(channel);

                ConcertResponse response = stub.updateConcert(request);
                channel.shutdown();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;

            } catch (Exception ex) {
                ex.printStackTrace();
                respond(false, "❌ Error forwarding to leader: " + ex.getMessage(), responseObserver);
                return;
            }
        }

        String concertId = request.getConcertId();
        System.out.println("📡 [Node " + port + "] Handling updateConcert for ID: " + concertId);

        try {
            DistributedLock lock = new DistributedLock(zkClient, concertId);
            try {
                lock.acquire();

                Concert concert = concertStore.get(concertId);
                if (concert == null) {
                    respond(false, "Concert not found", responseObserver);
                    return;
                }

                for (SeatTier tier : request.getSeatTiersList()) {
                    concert.addOrUpdateTier(tier.getTierName(), tier.getTotalSeats(), tier.getPrice());
                }

                concert.setAvailableAfterPartyTickets(request.getAfterPartyTickets());
                respond(true, "Concert updated successfully", responseObserver);

                List<String> followers = EtcdHelper.getOtherNodes("http://localhost:2379", port);
                for (String follower : followers) {
                    try {
                        ManagedChannel ch = ManagedChannelBuilder.forTarget(follower)
                                .usePlaintext().build();
                        ConcertServiceGrpc.ConcertServiceBlockingStub stub = ConcertServiceGrpc.newBlockingStub(ch);
                        stub.syncUpdateConcert(request);
                        ch.shutdown();
                        System.out.println("📡 Synced update to: " + follower);
                    } catch (Exception e) {
                        System.err.println("⚠️ Update sync failed for: " + follower + " → " + e.getMessage());
                    }
                }

            } catch (Exception e) {
                e.printStackTrace();
                respond(false, "ZooKeeper lock error: " + e.getMessage(), responseObserver);
            } finally {
                lock.release();
            }
        } catch (Exception e) {
            e.printStackTrace();
            respond(false, "ZooKeeper lock error: " + e.getMessage(), responseObserver);
        }
    }


//    @Override
//    public void cancelConcert(CancelConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {
//        if (concertStore.remove(request.getConcertId()) != null) {
//            responseObserver.onNext(ConcertResponse.newBuilder()
//                    .setSuccess(true)
//                    .setMessage("Concert canceled")
//                    .build());
//        } else {
//            responseObserver.onNext(ConcertResponse.newBuilder()
//                    .setSuccess(false)
//                    .setMessage("Concert not found")
//                    .build());
//        }
//        responseObserver.onCompleted();
//    }

    @Override
    public void cancelConcert(CancelConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {

        if (!isLeader) {
            try {
                String leaderAddress = leaderElection.getLeaderAddress();
                if (leaderAddress == null) {
                    respond(false, "❌ Leader unavailable. Try again.", responseObserver);
                    return;
                }

                System.out.println("📡 [Node " + port + "] Forwarding addConcert to leader: " + leaderAddress);

                ManagedChannel channel = ManagedChannelBuilder.forTarget(leaderAddress)
                        .usePlaintext()
                        .build();

                ConcertServiceGrpc.ConcertServiceBlockingStub stub =
                        ConcertServiceGrpc.newBlockingStub(channel);

                ConcertResponse response = stub.cancelConcert(request);
                channel.shutdown();

                responseObserver.onNext(response);
                responseObserver.onCompleted();
                return;

            } catch (Exception ex) {
                ex.printStackTrace();
                respond(false, "❌ Error forwarding to leader: " + ex.getMessage(), responseObserver);
                return;
            }
        }

        String concertId = request.getConcertId();
        System.out.println("📡 [Node " + port + "] Handling cancelConcert for ID: " + concertId);

        try {
            DistributedLock lock = new DistributedLock(zkClient, concertId);
            try {
                lock.acquire();

                if (concertStore.remove(concertId) != null) {
                    respond(true, "Concert canceled", responseObserver);
                } else {
                    respond(false, "Concert not found", responseObserver);
                }

                List<String> followers = EtcdHelper.getOtherNodes("http://localhost:2379", port);
                for (String follower : followers) {
                    try {
                        ManagedChannel ch = ManagedChannelBuilder.forTarget(follower)
                                .usePlaintext().build();
                        ConcertServiceGrpc.ConcertServiceBlockingStub stub = ConcertServiceGrpc.newBlockingStub(ch);
                        stub.syncCancelConcert(request);
                        ch.shutdown();
                        System.out.println("📡 Synced cancel to: " + follower);
                    } catch (Exception e) {
                        System.err.println("⚠️ Cancel sync failed for: " + follower + " → " + e.getMessage());
                    }
                }


            } catch (Exception e) {
                e.printStackTrace();
                respond(false, "ZooKeeper lock error: " + e.getMessage(), responseObserver);
            } finally {
                lock.release();
            }
        } catch (Exception e) {
            e.printStackTrace();
            respond(false, "ZooKeeper lock error: " + e.getMessage(), responseObserver);
        }
    }

    @Override
    public void syncConcert(AddConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {
        String concertId = request.getConcertId();

        Concert concert = new Concert(concertId, request.getConcertName(), request.getAfterPartyTickets());
        for (SeatTier tier : request.getSeatTiersList()) {
            concert.addOrUpdateTier(tier.getTierName(), tier.getTotalSeats(), tier.getPrice());
        }

        concertStore.put(concertId, concert);
        System.out.println("🔁 [Node " + port + "] Synced concert from leader: " + concertId);

        responseObserver.onNext(ConcertResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Synced successfully")
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void syncUpdateConcert(UpdateConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {
        String concertId = request.getConcertId();
        Concert concert = concertStore.get(concertId);

        if (concert == null) {
            responseObserver.onNext(ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Concert not found")
                    .build());
            responseObserver.onCompleted();
            return;
        }

        for (SeatTier tier : request.getSeatTiersList()) {
            concert.addOrUpdateTier(tier.getTierName(), tier.getTotalSeats(), tier.getPrice());
        }

        concert.setAvailableAfterPartyTickets(request.getAfterPartyTickets());

        System.out.println("🔁 Synced update for concert: " + concertId);

        responseObserver.onNext(ConcertResponse.newBuilder()
                .setSuccess(true)
                .setMessage("Update synced")
                .build());
        responseObserver.onCompleted();
    }

    @Override
    public void syncCancelConcert(CancelConcertRequest request, StreamObserver<ConcertResponse> responseObserver) {
        String concertId = request.getConcertId();

        if (concertStore.remove(concertId) != null) {
            System.out.println("🗑️ Synced cancellation of concert: " + concertId);
            responseObserver.onNext(ConcertResponse.newBuilder()
                    .setSuccess(true)
                    .setMessage("Cancel synced")
                    .build());
        } else {
            responseObserver.onNext(ConcertResponse.newBuilder()
                    .setSuccess(false)
                    .setMessage("Concert not found")
                    .build());
        }
        responseObserver.onCompleted();
    }

    @Override
    public void fetchAllConcerts(Empty request, StreamObserver<AllConcertsResponse> responseObserver) {
        AllConcertsResponse.Builder builder = AllConcertsResponse.newBuilder();
        for (Concert concert : concertStore.values()) {
            AddConcertRequest.Builder concertBuilder = AddConcertRequest.newBuilder()
                    .setConcertId(concert.getConcertId())
                    .setConcertName(concert.getConcertName())
                    .setAfterPartyTickets(concert.getAvailableAfterPartyTickets());
            for (Concert.SeatTier tier : concert.getSeatTiers().values()) {
                concertBuilder.addSeatTiers(SeatTier.newBuilder()
                        .setTierName(tier.getTierName())
                        .setTotalSeats(tier.getAvailableSeats()) // use available, or total if tracked
                        .setPrice(tier.getPrice())
                        .build());
            }
            builder.addConcerts(concertBuilder.build());
        }

        responseObserver.onNext(builder.build());
        responseObserver.onCompleted();
    }

    private void respond(boolean success, String message, StreamObserver<ConcertResponse> responseObserver) {
        responseObserver.onNext(
                ConcertResponse.newBuilder()
                        .setSuccess(success)
                        .setMessage(message)
                        .build()
        );
        responseObserver.onCompleted();
    }
}
