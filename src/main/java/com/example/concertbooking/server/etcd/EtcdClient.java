package com.example.concertbooking.server.etcd;

import org.json.JSONArray;
import org.json.JSONObject;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class EtcdClient {

    private final String etcdUrl;

    public EtcdClient(String etcdUrl) {
        this.etcdUrl = etcdUrl;
    }

    public void registerNode(String nodeId, String address) {
        try {

            URL url = new URL(etcdUrl + "/v3/kv/put");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            JSONObject body = new JSONObject();
            body.put("key", base64(nodeId));
            body.put("value", base64(address));

            try (OutputStream os = conn.getOutputStream()) {
                byte[] input = body.toString().getBytes(StandardCharsets.UTF_8);
                os.write(input, 0, input.length);
            }

            int responseCode = conn.getResponseCode();
            if (responseCode == 200) {
                System.out.println("Registered node with etcd: " + nodeId);
            } else {
                System.out.println("etcd registration failed: HTTP " + responseCode);
            }

            conn.disconnect();
        } catch (Exception e) {
            System.out.println("etcd error: " + e.getMessage());
        }
    }

    public void registerNodeWithLease(String nodeId, String address) {
        try {
            // Step 1: Request a lease with TTL (e.g., 10 seconds)
            String leaseId = grantLease(10);

            if (leaseId == null) {
                System.out.println("Failed to obtain lease from etcd");
                return;
            }

            // Step 2: Register key with lease
            URL url = new URL(etcdUrl + "/v3/kv/put");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            JSONObject body = new JSONObject();
            body.put("key", base64(nodeId));
            body.put("value", base64(address));
            body.put("lease", leaseId);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.toString().getBytes(StandardCharsets.UTF_8));
            }

            if (conn.getResponseCode() == 200) {
                System.out.println("Registered node with etcd: " + nodeId + " (with lease)");
            } else {
                System.out.println("etcd registration failed: HTTP " + conn.getResponseCode());
            }

            conn.disconnect();

            // Step 3: Keep lease alive in background
            keepLeaseAlive(leaseId);

        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public List<String> getRegisteredNodes() {

        List<String> nodes = new ArrayList<>();
        try {
            URL url = new URL(etcdUrl + "/v3/kv/range");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            JSONObject body = new JSONObject();
            body.put("key", base64("/concert_nodes/"));
            body.put("range_end", base64(getPrefixEnd("/concert_nodes/")));

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.toString().getBytes(StandardCharsets.UTF_8));
            }

            BufferedReader reader = new BufferedReader(new InputStreamReader(conn.getInputStream()));
            StringBuilder responseBuilder = new StringBuilder();
            String line;
            while ((line = reader.readLine()) != null) {
                responseBuilder.append(line);
            }

            JSONObject response = new JSONObject(responseBuilder.toString());
            JSONArray kvs = response.optJSONArray("kvs");
            if (kvs != null) {
                for (int i = 0; i < kvs.length(); i++) {
                    JSONObject kv = kvs.getJSONObject(i);
                    String value = kv.getString("value");
                    String decoded = new String(Base64.getDecoder().decode(value), StandardCharsets.UTF_8);
                    nodes.add(decoded); // This is the node address
                }
            }

            conn.disconnect();
        } catch (Exception e) {
            System.out.println("etcd fetch error: " + e.getMessage());
        }

        return nodes;
    }

    private String grantLease(int ttlSeconds) {
        try {
            URL url = new URL(etcdUrl + "/v3/lease/grant");
            HttpURLConnection conn = (HttpURLConnection) url.openConnection();
            conn.setRequestMethod("POST");
            conn.setRequestProperty("Content-Type", "application/json");
            conn.setDoOutput(true);

            JSONObject body = new JSONObject();
            body.put("TTL", ttlSeconds);

            try (OutputStream os = conn.getOutputStream()) {
                os.write(body.toString().getBytes(StandardCharsets.UTF_8));
            }

            JSONObject response = new JSONObject(new String(conn.getInputStream().readAllBytes(), StandardCharsets.UTF_8));
            conn.disconnect();

            return response.get("ID").toString(); // Lease ID
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
    }

    private void keepLeaseAlive(String leaseId) {
        Executors.newSingleThreadScheduledExecutor().scheduleAtFixedRate(() -> {
            try {
                URL url = new URL(etcdUrl + "/v3/lease/keepalive");
                HttpURLConnection conn = (HttpURLConnection) url.openConnection();
                conn.setRequestMethod("POST");
                conn.setRequestProperty("Content-Type", "application/json");
                conn.setDoOutput(true);

                JSONObject body = new JSONObject();
                body.put("ID", leaseId);

                try (OutputStream os = conn.getOutputStream()) {
                    os.write(body.toString().getBytes(StandardCharsets.UTF_8));
                }

                conn.getInputStream().close();
                conn.disconnect();
            } catch (Exception e) {
                System.out.println("⚠Keepalive failed: " + e.getMessage());
            }
        }, 0, 5, TimeUnit.SECONDS); // ping every 5s
    }
    private String getPrefixEnd(String prefix) {
        byte[] bytes = prefix.getBytes(StandardCharsets.UTF_8);
        bytes[bytes.length - 1]++;
        return new String(bytes, StandardCharsets.UTF_8);
    }

    private String base64(String str) {
        return java.util.Base64.getEncoder().encodeToString(str.getBytes(StandardCharsets.UTF_8));
    }
}
