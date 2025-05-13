package com.example.concertbooking.server;

import com.example.concertbooking.grpc.*;
import com.example.concertbooking.server.etcd.EtcdClient;
import com.example.concertbooking.server.model.Concert;
import com.example.concertbooking.server.model.Reservation;
import com.example.concertbooking.server.zk.LeaderElection;
import com.example.concertbooking.server.zk.ZooKeeperClient;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Server;
import io.grpc.ServerBuilder;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ConcertServer {

    private Server server;

    public void start(int port) throws Exception {
        // ✅ Shared concert store
        Map<String, Concert> concertStore = new ConcurrentHashMap<>();
        Map<String, Reservation> reservations = new ConcurrentHashMap<>();

        ZooKeeperClient zkClient = new ZooKeeperClient();

        LeaderElection leaderElection = new LeaderElection(zkClient, port);

        EtcdClient etcdClient = new EtcdClient("http://localhost:2379");
        String nodeId = "/concert_nodes/node-" + port;
        String nodeAddress = "localhost:" + port;
        etcdClient.registerNodeWithLease(nodeId, nodeAddress);

        List<String> nodes = etcdClient.getRegisteredNodes();
        System.out.println("🎯 Nodes currently registered in etcd:");
        nodes.forEach(addr -> System.out.println(" - " + addr));

        // Re-sync from leader if this node is a follower
        if (!leaderElection.isLeader()) {
            String leaderAddress = leaderElection.getLeaderAddress();
            if (leaderAddress != null) {
                try {
                    // Fetch concerts
                    ManagedChannel ch1 = ManagedChannelBuilder.forTarget(leaderAddress).usePlaintext().build();
                    ConcertServiceGrpc.ConcertServiceBlockingStub concertStub =
                            ConcertServiceGrpc.newBlockingStub(ch1);
                    AllConcertsResponse concertResponse = concertStub.fetchAllConcerts(Empty.newBuilder().build());
                    for (AddConcertRequest req : concertResponse.getConcertsList()) {
                        concertStore.put(req.getConcertId(), new Concert(req));
                    }
                    ch1.shutdown();

                    // Fetch reservations
                    ManagedChannel ch2 = ManagedChannelBuilder.forTarget(leaderAddress).usePlaintext().build();
                    ReservationServiceGrpc.ReservationServiceBlockingStub reservationStub =
                            ReservationServiceGrpc.newBlockingStub(ch2);
                    AllReservationsResponse reservationResponse = reservationStub.fetchAllReservations(Empty.newBuilder().build());
                    for (ReserveRequest r : reservationResponse.getReservationsList()) {
                        String key = r.getUserId() + "-" + r.getConcertId();
                        reservations.put(key, new Reservation(r));
                    }
                    ch2.shutdown();

                    System.out.println("🔄 Re-sync completed from leader: " + leaderAddress);
                } catch (Exception e) {
                    System.err.println("⚠️ Re-sync failed: " + e.getMessage());
                }
            }
        }

        // Start the gRPC server
        server = ServerBuilder.forPort(port)
                .addService(new ConcertServiceImpl(concertStore, zkClient, port, leaderElection))
                .addService(new ReservationServiceImpl(concertStore, reservations, zkClient, port, leaderElection))
                .build()
                .start();

        System.out.println("🎵 Concert Ticket Reservation Server started on port " + port);

        // Shutdown hook
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.err.println("Shutting down gRPC server...");
            ConcertServer.this.stop();
            System.err.println("Server shut down.");
        }));

        server.awaitTermination();
    }

    public void stop() {
        if (server != null) {
            server.shutdown();
        }
    }

//    public static void main(String[] args) throws Exception {
//        int port = 50051;
//        if (args.length > 0) {
//            try {
//                port = Integer.parseInt(args[0]);
//            } catch (NumberFormatException e) {
//                System.err.println("Invalid port passed. Falling back to default port 50051.");
//            }
//        }
//
//        new ConcertServer().start(port);
//    }

    public static void main(String[] args) throws Exception {
        int port = 50051; // Default

        if (args.length > 0) {
            try {
                port = Integer.parseInt(args[0]);
            } catch (NumberFormatException e) {
                System.err.println("Invalid port. Using default 50051.");
            }
        }

        new ConcertServer().start(port); // Pass port to start()
    }
}
