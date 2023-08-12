package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class AdaptiveStrategy extends AbstractStrategy {


    public AdaptiveStrategy(int corePoolSize, int maxPoolSize) {
        _corePoolSize = corePoolSize;
        _maxPoolSize = maxPoolSize; 
    }

    @Override
    public <T> BlockingQueue<T> instantiate(Class<T> t) {
        return new LinkedBlockingQueue<T>(getMaxPoolSize());
    }

    public void setMaxPoolSize(int newMaxPoolSize) {
        _maxPoolSize = newMaxPoolSize; 
    }
    
}
