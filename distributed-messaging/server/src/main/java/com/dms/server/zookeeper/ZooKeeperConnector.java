package com.dms.server.zookeeper;

import com.dms.common.constants.Config;
import com.dms.common.model.NodeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

@Component
public class ZooKeeperConnector {
    private ZooKeeper zk;
    private final ObjectMapper M = new ObjectMapper().registerModule(new JavaTimeModule());
    private final ScheduledExecutorService heartbeatExecutor = Executors.newSingleThreadScheduledExecutor();

    @Value("${node.id:}")
    private String nodeId;

    @Value("${server.port:8080}")
    private int port;

    @Value("${server.host:localhost}")
    private String host;

    private String serversPath;
    private String heartbeatsPath;
    private String electionPath;
    private String myElectionZnode;
    private volatile String leaderId;

    @PostConstruct
    public void connect() throws IOException, KeeperException, InterruptedException {
        zk = new ZooKeeper(Config.ZK_CONNECT, 3000, event -> {});
        // ensure root path
        if (zk.exists(Config.ROOT, false) == null) {
            try { zk.create(Config.ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); } catch (KeeperException.NodeExistsException e) {}
        }
        serversPath = Config.ROOT + "/servers";
        if (zk.exists(serversPath, false) == null) {
            try { zk.create(serversPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); } catch (KeeperException.NodeExistsException e) {}
        }
        heartbeatsPath = Config.ROOT + "/heartbeats";
        if (zk.exists(heartbeatsPath, false) == null) {
            try { zk.create(heartbeatsPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); } catch (KeeperException.NodeExistsException e) {}
        }
        electionPath = Config.ROOT + "/election";
        if (zk.exists(electionPath, false) == null) {
            try { zk.create(electionPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); } catch (KeeperException.NodeExistsException e) {}
        }

        // generate stable node ID if not provided (aligns with UI expectations)
        if (nodeId == null || nodeId.isEmpty()) {
            nodeId = "server-" + port;
        }
        
        // create ephemeral znode for this server
        NodeInfo info = new NodeInfo(nodeId, host, port);
        byte[] data = M.writeValueAsBytes(info);
        String path = serversPath + "/" + nodeId;
        if (zk.exists(path, false) != null) {
            zk.delete(path, -1);
        }
        zk.create(path, data, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        System.out.println("Registered server: " + nodeId + " at " + host + ":" + port);

        // create heartbeat node and schedule updates
        String hbPath = heartbeatsPath + "/" + nodeId;
        byte[] hbData = Instant.now().toString().getBytes(StandardCharsets.UTF_8);
        if (zk.exists(hbPath, false) == null) {
            zk.create(hbPath, hbData, ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
        } else {
            zk.setData(hbPath, hbData, -1);
        }
        heartbeatExecutor.scheduleAtFixedRate(() -> {
            try {
                byte[] ts = Instant.now().toString().getBytes(StandardCharsets.UTF_8);
                zk.setData(hbPath, ts, -1);
            } catch (Exception ignored) {}
        }, 0, 2, TimeUnit.SECONDS);

        // participate in leader election
        String seqPath = electionPath + "/node-";
        myElectionZnode = zk.create(seqPath, nodeId.getBytes(StandardCharsets.UTF_8), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL_SEQUENTIAL);
        updateLeader();
    }

    public ZooKeeper getZooKeeper() { return zk; }

    public String getNodeId() { return nodeId; }

    public String getServersPath() { return serversPath; }

    public String getHeartbeatsPath() { return heartbeatsPath; }
    public String getElectionPath() { return electionPath; }
    public String getLeaderId() { return leaderId; }
    public boolean isLeader() { return nodeId != null && nodeId.equals(leaderId); }

    private void updateLeader() {
        try {
            List<String> children = zk.getChildren(electionPath, false);
            if (children.isEmpty()) { leaderId = nodeId; return; }
            children.sort(String::compareTo);
            String smallest = children.get(0);
            byte[] data = zk.getData(electionPath + "/" + smallest, false, null);
            leaderId = new String(data, StandardCharsets.UTF_8);
        } catch (Exception e) {
            leaderId = nodeId;
        }
    }

    @PreDestroy
    public void close() throws InterruptedException {
        heartbeatExecutor.shutdownNow();
        if (zk != null) zk.close();
    }
}
