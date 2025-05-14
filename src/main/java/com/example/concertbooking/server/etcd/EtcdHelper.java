package com.example.concertbooking.server.etcd;

import org.json.JSONArray;
import org.json.JSONObject;

import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Base64;
import java.util.List;

public class EtcdHelper {

    public static List<String> getOtherNodes(String etcdUrl, int currentPort) {
        List<String> others = new ArrayList<>();
        try {
            URL url = new URL(etcdUrl + "/v3/kv/range");
            HttpURLConnection con = (HttpURLConnection) url.openConnection();
            con.setRequestMethod("POST");
            con.setRequestProperty("Content-Type", "application/json");

            String key = Base64.getEncoder().encodeToString("/concert_nodes/".getBytes());
            String requestBody = "{\"key\":\"" + key + "\",\"range_end\":\"" +
                    Base64.getEncoder().encodeToString("/concert_nodes0".getBytes()) + "\"}";

            con.setDoOutput(true);
            con.getOutputStream().write(requestBody.getBytes());

            String response = new String(con.getInputStream().readAllBytes());
            JSONObject obj = new JSONObject(response);

            JSONArray kvs = obj.getJSONArray("kvs");
            for (int i = 0; i < kvs.length(); i++) {
                JSONObject kv = kvs.getJSONObject(i);
                String value = new String(Base64.getDecoder().decode(kv.getString("value")));
                if (!value.endsWith(String.valueOf(currentPort))) {
                    others.add(value);
                }
            }

        } catch (Exception e) {
            System.err.println("Error retrieving etcd nodes: " + e.getMessage());
        }
        return others;
    }
}
