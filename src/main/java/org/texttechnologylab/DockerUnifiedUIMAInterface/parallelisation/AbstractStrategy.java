package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

public abstract class AbstractStrategy implements PoolStrategy{
    int _corePoolSize;
    int _maxPoolSize; 

    @Override
    public int getMaxPoolSize() {
        return _maxPoolSize; 
    }

    @Override
    public int getCorePoolSize() {
        return _corePoolSize; 
    }
}
