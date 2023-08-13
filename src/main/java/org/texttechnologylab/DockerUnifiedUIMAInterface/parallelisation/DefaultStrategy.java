package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

public class DefaultStrategy implements PoolStrategy {

    @Override
    public <T> BlockingQueue<T> instantiate(Class<T> t) {
        return new LinkedBlockingQueue<T>(getMaxPoolSize());
    }
}
