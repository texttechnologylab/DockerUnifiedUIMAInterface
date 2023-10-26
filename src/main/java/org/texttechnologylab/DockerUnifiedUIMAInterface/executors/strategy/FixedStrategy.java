package org.texttechnologylab.DockerUnifiedUIMAInterface.executors.strategy;

import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

public class FixedStrategy extends AbstractStrategy {

    public FixedStrategy(int poolSize) {
        _maxPoolSize = _corePoolSize = poolSize; 
    }

    @Override
    public <T> BlockingQueue<T> instantiate(Class<T> t) {
        return new ArrayBlockingQueue<T>(getMaxPoolSize());
    }

    @Override
    public int getInitialQueueSize() {
        return _maxPoolSize;
    }
}
