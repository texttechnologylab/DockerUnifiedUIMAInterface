package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;

import java.time.Instant;

public record DUUIEvent(Sender sender, String message, long timestamp, DUUIComposer.DebugLevel debugLevel) {

    public DUUIEvent(Sender sender, String message) {
        this(sender, message, Instant.now().toEpochMilli(), DUUIComposer.DebugLevel.NONE);
    }

    public DUUIEvent(Sender sender, String message, DUUIComposer.DebugLevel debugLevel) {
        this(sender, message, Instant.now().toEpochMilli(), debugLevel);
    }


    public DUUIEvent(Sender sender, String message, long timestamp) {
        this(sender, message, timestamp, DUUIComposer.DebugLevel.NONE);
    }

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
        MOINTOR
    }

    @Override
    public String toString() {
        return String.format("%s [%s]: %s", timestamp, sender.name(), message);
    }
}
