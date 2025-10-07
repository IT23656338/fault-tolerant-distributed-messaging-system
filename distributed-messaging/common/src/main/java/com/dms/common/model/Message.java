package com.dms.common.model;

import java.io.Serializable;
import java.time.Instant;
import java.util.UUID;

public class Message implements Serializable {
    private String id;
    private String sender;
    private String receiver;
    private String payload;
    private Instant timestamp;

    public Message() {
        this.id = UUID.randomUUID().toString();
        this.timestamp = Instant.now();
    }

    public Message(String sender, String receiver, String payload) {
        this();
        this.sender = sender;
        this.receiver = receiver;
        this.payload = payload;
    }

    // getters and setters
    public String getId() { return id; }
    public String getSender() { return sender; }
    public void setSender(String sender) { this.sender = sender; }
    public String getReceiver() { return receiver; }
    public void setReceiver(String receiver) { this.receiver = receiver; }
    public String getPayload() { return payload; }
    public void setPayload(String payload) { this.payload = payload; }
    public Instant getTimestamp() { return timestamp; }
    public void setTimestamp(Instant timestamp) { this.timestamp = timestamp; }
}
