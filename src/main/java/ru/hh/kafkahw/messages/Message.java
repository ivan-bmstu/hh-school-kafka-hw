package ru.hh.kafkahw.messages;

import java.util.UUID;

public class Message {

    private String content;
    private UUID uuid;

    public Message() {
    }

    public Message(String content, UUID uuid) {
        this.content = content;
        this.uuid = uuid;
    }

    public String getContent() {
        return content;
    }

    public UUID getUuid() {
        return uuid;
    }

    public Message setContent(String content) {
        this.content = content;
        return this;
    }

    public Message setUuid(UUID uuid) {
        this.uuid = uuid;
        return this;
    }
}
