package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.strategy;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

    public interface PoolStrategy {

    <T> BlockingQueue<T> instantiate(Class<T> t);
    
    int getInitialQueueSize();

    default long getTimeout(TimeUnit unit) {
        return unit.convert(500, TimeUnit.MILLISECONDS);
    };

    
    default int getMaxPoolSize() {
        return ((int) Math.round(Runtime.getRuntime().availableProcessors() * 1.5));
    }; 

    default int getCorePoolSize() {
        return Runtime.getRuntime().availableProcessors();
    };
}