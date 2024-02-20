package ru.hh.kafkahw.consumer;

import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

public class SetUUIDMessages {

    private final Set<UUID> setsUUID;

    public SetUUIDMessages() {
        this.setsUUID = new HashSet<>();
    }

    public boolean isNew(UUID id){
        return !setsUUID.contains(id);
    }

    public void writeId(UUID id){
        setsUUID.add(id);
    }
}
