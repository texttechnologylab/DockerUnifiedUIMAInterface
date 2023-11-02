package org.texttechnologylab.DockerUnifiedUIMAInterface.executors.strategy;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

public class AdaptiveStrategy extends AbstractStrategy {

    final int _casPoolSize;

    public AdaptiveStrategy() {
        this(1, Integer.MAX_VALUE);
    }

    public AdaptiveStrategy(int casPoolSize, int corePoolSize, int maxPoolSize) {
        if (_corePoolSize > maxPoolSize) {
            throw new IllegalArgumentException(
                String.format("Max pool size has to be greater than core pool size:\n Core size: %d | Max size: %d", 
                _corePoolSize, maxPoolSize));
        }
        if (casPoolSize < 0) {
            throw new IllegalArgumentException(
                String.format("Cas pool size must be greater than or equal 0: %d", 
                casPoolSize));
        }

        _corePoolSize = corePoolSize;
        _maxPoolSize = maxPoolSize; 
        _casPoolSize = casPoolSize;
    }

    public AdaptiveStrategy(int corePoolSize, int maxPoolSize) {
        this(Integer.MAX_VALUE, corePoolSize, maxPoolSize);
    }


    @Override
    public <T> BlockingQueue<T> instantiate(Class<T> t) {
        return new LinkedBlockingQueue<T>(_casPoolSize);
    }

    @Override
    public int getInitialQueueSize() {
        return _casPoolSize;
    }

    /**
     * Dynamically set max-pool-size.
     * 
     * @param newMaxPoolSize
     */
    public void setMaxPoolSize(int newMaxPoolSize) {
        _maxPoolSize = newMaxPoolSize; 
    }

    /**
     * Dynamically set core-pool-size.
     * 
     * @param newCorePoolSize
     */
    public void setCorePoolSize(int newCorePoolSize) {
        _corePoolSize = newCorePoolSize; 
    }

    @Override
    public long getTimeout(TimeUnit unit) {
        return unit.convert(3000, TimeUnit.MILLISECONDS);
    }
    
}
