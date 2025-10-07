package com.dms.server.service;

import com.dms.common.model.Message;
import com.dms.server.repository.MessageRepository;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.Set;

@Service
public class MessageService {
    private final MessageRepository repo;
    private final ReplicationService repl;
    private final Set<String> seen = new HashSet<>();

    public MessageService(MessageRepository repo, ReplicationService repl) {
        this.repo = repo; this.repl = repl;
    }

    public Message handleMessage(Message m) {
        if (seen.contains(m.getId())) {
            System.out.println("Duplicate message ignored: " + m.getId());
            return m;
        }
        seen.add(m.getId());
        repo.save(m);
        
        // Handle broadcast messages
        if ("BROADCAST".equals(m.getReceiver())) {
            repl.broadcast(m);
            System.out.println("Stored and broadcasted message: " + m.getId());
        } else {
            repl.replicate(m);
            System.out.println("Stored and replicated message: " + m.getId());
        }
        return m;
    }

    // endpoint used by replicas to accept replicated messages
    public Message acceptReplica(Message m) {
        if (!seen.contains(m.getId())) { 
            repo.save(m); 
            seen.add(m.getId());
            System.out.println("Received replicated message: " + m.getId() + " from " + m.getSender() + " to " + m.getReceiver());
        } else {
            System.out.println("Duplicate replicated message ignored: " + m.getId());
        }
        return m;
    }
}
