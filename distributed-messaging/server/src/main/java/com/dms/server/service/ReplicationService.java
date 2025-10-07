package com.dms.server.service;

import com.dms.common.constants.Config;
import com.dms.common.model.Message;
import com.dms.common.model.NodeInfo;
import com.dms.server.zookeeper.ZooKeeperConnector;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.MediaType;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

import javax.annotation.PostConstruct;
import java.util.ArrayList;
import java.util.List;

@Service
public class ReplicationService {
    private final List<String> replicas = new ArrayList<>();
    private final ZooKeeperConnector connector;
    private final RestTemplate http = new RestTemplate();
    private final ObjectMapper M = new ObjectMapper().registerModule(new JavaTimeModule());

    public ReplicationService(ZooKeeperConnector connector) {
        this.connector = connector;
    }

    public void replicate(Message m) {
        try {
            ZooKeeper zk = connector.getZooKeeper();
            List<String> children = zk.getChildren(Config.ROOT + "/servers", false);
            List<String> current = new ArrayList<>();
            String selfId = connector.getNodeId();
            for (String c : children) {
                if (c.equals(selfId)) continue;
                byte[] data = zk.getData(Config.ROOT + "/servers/" + c, false, null);
                NodeInfo ni = M.readValue(data, NodeInfo.class);
                current.add("http://" + ni.getHost() + ":" + ni.getPort());
            }
            System.out.println("Replicating message " + m.getId() + " to " + current);
            String json = M.writeValueAsString(m);
            HttpHeaders headers = new HttpHeaders();
            headers.setContentType(MediaType.APPLICATION_JSON);
            HttpEntity<String> ent = new HttpEntity<>(json, headers);
            for (String r : current) {
                try { 
                    http.postForObject(r + "/api/messages/replica", ent, String.class);
                    System.out.println("Successfully replicated message " + m.getId() + " to " + r);
                } catch (Exception e) { 
                    System.out.println("Failed to replicate message " + m.getId() + " to " + r + ": " + e.getMessage());
                }
            }
        } catch (Exception e) {
            System.out.println("Error during replication: " + e.getMessage());
            e.printStackTrace();
        }
    }

    public void broadcast(Message m) {
        try {
            ZooKeeper zk = connector.getZooKeeper();
            List<String> children = zk.getChildren(Config.ROOT + "/servers", false);
            List<String> current = new ArrayList<>();
            String selfId = connector.getNodeId();
            for (String c : children) {
                if (c.equals(selfId)) continue;
                byte[] data = zk.getData(Config.ROOT + "/servers/" + c, false, null);
                NodeInfo ni = M.readValue(data, NodeInfo.class);
                current.add("http://" + ni.getHost() + ":" + ni.getPort());
            }
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
}
