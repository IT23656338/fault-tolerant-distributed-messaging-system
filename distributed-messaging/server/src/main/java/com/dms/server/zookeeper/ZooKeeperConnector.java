package com.dms.server.zookeeper;

import com.dms.common.constants.Config;
import com.dms.common.model.NodeInfo;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

@Component
public class ZooKeeperConnector {
    private ZooKeeper zk;
    private final ObjectMapper M = new ObjectMapper().registerModule(new JavaTimeModule());

    @Value("${node.id:}")
    private String nodeId;

    @Value("${server.port:8080}")
    private int port;

    @Value("${server.host:localhost}")
    private String host;

    @PostConstruct
    public void connect() throws IOException, KeeperException, InterruptedException {
        zk = new ZooKeeper(Config.ZK_CONNECT, 3000, event -> {});
        // ensure root path
        if (zk.exists(Config.ROOT, false) == null) {
            try { zk.create(Config.ROOT, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); } catch (KeeperException.NodeExistsException e) {}
        }
        String serversPath = Config.ROOT + "/servers";
        if (zk.exists(serversPath, false) == null) {
            try { zk.create(serversPath, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT); } catch (KeeperException.NodeExistsException e) {}
        }

        // generate unique node ID if not provided
        if (nodeId == null || nodeId.isEmpty()) {
            nodeId = "server-" + port + "-" + System.currentTimeMillis();
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
    }

    public ZooKeeper getZooKeeper() { return zk; }

    public String getNodeId() { return nodeId; }

    @PreDestroy
    public void close() throws InterruptedException {
        if (zk != null) zk.close();
    }
}
