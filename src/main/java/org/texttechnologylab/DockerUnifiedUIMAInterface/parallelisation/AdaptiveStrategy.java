package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class AdaptiveStrategy extends AbstractStrategy {


    public AdaptiveStrategy() {
        this(Runtime.getRuntime().availableProcessors(), Integer.MAX_VALUE);
    }

    public AdaptiveStrategy(int corePoolSize, int maxPoolSize) {
        if (_corePoolSize > maxPoolSize) {
            throw new IllegalArgumentException(
                String.format("Max pool size has to be greater than core pool size:\n Core size: %d | Max size: %d", 
                _corePoolSize, maxPoolSize));
        }
        _corePoolSize = corePoolSize;
        _maxPoolSize = maxPoolSize; 
    }


    @Override
    public <T> BlockingQueue<T> instantiate(Class<T> t) {
        return new LinkedBlockingQueue<T>();
    }

    public void setMaxPoolSize(int newMaxPoolSize) {
        _maxPoolSize = newMaxPoolSize; 
    }

    public long getTimeout(TimeUnit unit) {
        return unit.convert(100, TimeUnit.MILLISECONDS);
    }
    
}
