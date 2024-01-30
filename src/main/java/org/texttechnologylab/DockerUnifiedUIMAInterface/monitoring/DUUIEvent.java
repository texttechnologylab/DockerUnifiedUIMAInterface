package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.time.Instant;

public class DUUIEvent {

    public enum Sender {
        SYSTEM,
        COMPOSER,
        DRIVER,
        COMPONENT,
        HANDLER,
        DOCUMENT,
        STORAGE,
        READER,
        WRITER,
        MOINTOR;
    }

    private final Sender sender;
    private final String message;
    private final long timestamp;
    private final DUUIComposer.DebugLevel debugLevel;

    public DUUIEvent(Sender sender, String message) {
        this.sender = sender;
        this.message = message;
        this.timestamp = Instant.now().toEpochMilli();
        this.debugLevel = DUUIComposer.DebugLevel.NONE;
    }


    public DUUIEvent(Sender sender, String message, DUUIComposer.DebugLevel debugLevel) {
        this.sender = sender;
        this.message = message;
        this.timestamp = Instant.now().toEpochMilli();
        this.debugLevel = debugLevel;
    }

    public DUUIEvent(Sender sender, String message, long timestamp) {
        this.sender = sender;
        this.message = message;
        this.timestamp = timestamp;
        this.debugLevel = DUUIComposer.DebugLevel.NONE;
    }

    public DUUIEvent(Sender sender, String message, long timestamp, DUUIComposer.DebugLevel debugLevel) {
        this.sender = sender;
        this.message = message;
        this.timestamp = timestamp;
        this.debugLevel = debugLevel;
    }

    @Override
    public String toString() {
        return String.format("%s [%s]: %s", timestamp, sender.name(), message);
    }

    public Sender getSender() {
        return sender;
    }

    public String getMessage() {
        return message;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public DUUIComposer.DebugLevel getDebugLevel() {
        return debugLevel;
    }
}
