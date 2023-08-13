package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;

public interface PoolStrategy {

    <T> BlockingQueue<T> instantiate(Class<T> t);

    default long getTimeOut(TimeUnit unit) {
        return TimeUnit.SECONDS.convert(3, unit);
    };
    
    default int getMaxPoolSize() {
        return ((int) Math.round(Runtime.getRuntime().availableProcessors() * 1.5));
    }; 

    default int getCorePoolSize() {
        return Runtime.getRuntime().availableProcessors();
    };
}