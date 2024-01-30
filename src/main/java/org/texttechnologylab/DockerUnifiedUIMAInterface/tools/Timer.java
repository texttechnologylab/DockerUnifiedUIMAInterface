package org.texttechnologylab.DockerUnifiedUIMAInterface.tools;

import java.time.Instant;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

public class Timer {

    private long startTime = 0L;
    private long endTime = 0L;

    public void start() {
        startTime = Instant.now().toEpochMilli();
    }

    public void stop() {
        endTime = Instant.now().toEpochMilli();
    }

    public long getDuration() {
        if (startTime >= endTime) return 0;
        return (endTime - startTime);
    }

    public void restart() {
        endTime = 0L;
        start();
    }
}
