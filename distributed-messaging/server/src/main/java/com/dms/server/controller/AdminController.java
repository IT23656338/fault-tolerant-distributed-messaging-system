package com.dms.server.controller;

import com.dms.common.constants.Config;
import com.dms.common.model.NodeInfo;
import com.dms.server.zookeeper.ZooKeeperConnector;
import com.dms.server.repository.MessageRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import org.apache.zookeeper.ZooKeeper;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.ArrayList;
import java.util.List;

@RestController
@RequestMapping("/admin")
public class AdminController {

    private final ZooKeeperConnector connector;
    private final ObjectMapper M = new ObjectMapper().registerModule(new JavaTimeModule());

    private final MessageRepository messageRepository;

    public AdminController(ZooKeeperConnector connector, MessageRepository messageRepository) {
        this.connector = connector;
        this.messageRepository = messageRepository;
    }

    @GetMapping("/nodes")
    public ResponseEntity<List<NodeInfo>> nodes() throws Exception {
        ZooKeeper zk = connector.getZooKeeper();
        List<String> children = zk.getChildren(Config.ROOT + "/servers", false);
        List<NodeInfo> out = new ArrayList<>();
        for (String c : children) {
            byte[] data = zk.getData(Config.ROOT + "/servers/" + c, false, null);
            NodeInfo ni = M.readValue(data, NodeInfo.class);
            out.add(ni);
        }
        return ResponseEntity.ok(out);
    }

    @GetMapping("/messages")
    public ResponseEntity<List<com.dms.common.model.Message>> messages() {
        return ResponseEntity.ok(messageRepository.findAll());
    }
}
