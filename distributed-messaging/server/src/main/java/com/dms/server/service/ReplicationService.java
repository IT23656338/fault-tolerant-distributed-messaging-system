package com.dms.server.service;

import com.dms.common.model.Message;
import com.dms.common.model.NodeInfo;
import com.dms.server.zookeeper.ZooKeeperConnector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.Watcher;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

@Service
public class ReplicationService {
    private final List<String> replicas = new CopyOnWriteArrayList<>();
    private final ZooKeeperConnector connector;
    private final RestTemplate http = new RestTemplate();
    private final ObjectMapper M = new ObjectMapper().registerModule(new JavaTimeModule());
    private volatile boolean partitionMode = false;
    private final List<Message> sentLog = new CopyOnWriteArrayList<>();
    private static final int MAX_LOG_SIZE = 500;

    public ReplicationService(ZooKeeperConnector connector) {
        this.connector = connector;
    }

    @PostConstruct
    private void initMembershipWatch() {
        try {
            refreshReplicas();
            ZooKeeper zk = connector.getZooKeeper();
            String serversPath = connector.getServersPath();
            zk.getChildren(serversPath, (Watcher) event -> {
                try { refreshReplicas(); } catch (Exception ignored) {}
            });
        } catch (Exception e) {
            System.out.println("Failed to initialize membership watch: " + e.getMessage());
        }
    }

    private void refreshReplicas() throws Exception {
        ZooKeeper zk = connector.getZooKeeper();
        String serversPath = connector.getServersPath();
        List<String> children = zk.getChildren(serversPath, false);
        List<String> current = new ArrayList<>();
        String selfId = connector.getNodeId();
        for (String c : children) {
            if (c.equals(selfId)) continue;
            byte[] data = zk.getData(serversPath + "/" + c, false, null);
            NodeInfo ni = M.readValue(data, NodeInfo.class);
            current.add("http://" + ni.getHost() + ":" + ni.getPort());
        }
        List<String> old = new ArrayList<>(replicas);
        replicas.clear();
        replicas.addAll(current);
        System.out.println("Replica membership updated: " + replicas);
        // replay to new nodes
        List<String> added = new ArrayList<>(current);
        added.removeAll(old);
        if (!added.isEmpty()) { replayTo(added); }
    }

    public void setPartitionMode(boolean enabled) { this.partitionMode = enabled; }
    public boolean isPartitionMode() { return partitionMode; }

    private void recordSent(Message m) {
        sentLog.add(m);
        if (sentLog.size() > MAX_LOG_SIZE) {
            sentLog.remove(0);
        }
    }

    public void replayTo(List<String> targets) {
        for (Message m : sentLog) {
            try {
                String json = M.writeValueAsString(m);
                HttpHeaders headers = new HttpHeaders();
                headers.setContentType(MediaType.APPLICATION_JSON);
                HttpEntity<String> ent = new HttpEntity<>(json, headers);
                for (String r : targets) {
                    try { http.postForObject(r + "/api/messages/replica", ent, String.class); } catch (Exception ignored) {}
                }
            } catch (Exception ignored) {}
        }
    }

    public void replicate(Message m) {
        try {
            List<String> current = new ArrayList<>(replicas);
            System.out.println("Replicating message " + m.getId() + " to " + current);
            String json = M.writeValueAsString(m);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> ent = new HttpEntity<>(json, headers);
            int acks = 1; // self write
            int need = (current.size() + 1) / 2 + 1; // majority quorum
            for (String r : current) {
                try {
                    ResponseEntity<String> resp = http.postForEntity(r + "/api/messages/replica", ent, String.class);
                    if (resp.getStatusCode().is2xxSuccessful()) {
                        acks++;
                        System.out.println("Ack from replica " + r + " for message " + m.getId());
                    }
                } catch (Exception e) {
                    System.out.println("Failed to replicate message " + m.getId() + " to " + r + ": " + e.getMessage());
                }
            }
            recordSent(m);
            if (!partitionMode && acks < need) {
                System.out.println("Warning: quorum not reached for message " + m.getId() + ". acks=" + acks + "/" + need);
            }
        } catch (Exception e) {
            System.out.println("Error during replication: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void broadcast(Message m) {
        try {
            List<String> current = new ArrayList<>(replicas);
            System.out.println("Broadcasting message " + m.getId() + " to " + current);
            String json = M.writeValueAsString(m);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> ent = new HttpEntity<>(json, headers);
            for (String r : current) {
                try { 
                    http.postForObject(r + "/api/messages/replica", ent, String.class);
                    System.out.println("Successfully broadcasted message " + m.getId() + " to " + r);
                } catch (Exception e) { 
                    System.out.println("Failed to broadcast message " + m.getId() + " to " + r + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println("Error during broadcast: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void replicateToSingle(Message m, String targetNodeId) {
        try {
            // map from nodeId to URL by reading servers znodes
            ZooKeeper zk = connector.getZooKeeper();
            String serversPath = connector.getServersPath();
            byte[] data = zk.getData(serversPath + "/" + targetNodeId, false, null);
            NodeInfo ni = M.readValue(data, NodeInfo.class);
            String url = "http://" + ni.getHost() + ":" + ni.getPort();
            String json = M.writeValueAsString(m);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> ent = new HttpEntity<>(json, headers);
            http.postForObject(url + "/api/messages/replica", ent, String.class);
            System.out.println("Unicast replicated message " + m.getId() + " to " + targetNodeId + " at " + url);
        } catch (Exception e) {
            System.out.println("Failed unicast replication to " + targetNodeId + ": " + e.getMessage());
        }
    }
}
