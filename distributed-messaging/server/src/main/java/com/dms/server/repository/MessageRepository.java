package com.dms.server.repository;

import com.dms.common.model.Message;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.List;

@Repository
public class MessageRepository {
    private final List<Message> store = new ArrayList<>();
    public void save(Message m) { store.add(m); }
    public List<Message> findAll() { return new ArrayList<>(store); }
}
