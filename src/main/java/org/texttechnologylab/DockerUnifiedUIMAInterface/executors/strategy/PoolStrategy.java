package org.texttechnologylab.DockerUnifiedUIMAInterface.executors.strategy;

import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;


/**
 * Strategy which acts as a global config for the pipeline and other aspects of DUUI.
 * Pools are queues containing resources in a multithreaded environment. Therefore, these 
 * resources are managed in a {@link BlockingQueue}. 
 * 
 * In the future, it might be beneficial to switch to non-blocking thread-safe queues, however, 
 * these implementations noticeably increase complexity.
 * 
 * @see AdaptiveStrategy
 * @see FixedStrategy
 * 
 */
public interface PoolStrategy {

    /**
     * Instantiate the {@link BlockingQueue} using a provided implementation {@link T}.
     * 
     * @param <T> Implementation of a {@link BlockingQueue}.
     * @param t Class of the implementation.
     * @return Instantiated queue.
     */
    <T> BlockingQueue<T> instantiate(Class<T> t);


    /**
     * Initial pool size currently used for the {@link ResourceManager.CasPool}. 
     * 
     * @return CAS-pool-size.
     */
    int getInitialQueueSize();

    /**
     * Timeout primarily used by {@link ThreadPoolExecutors}. Determines how long an idle 
     * thread stays alive.
     * 
     * @param unit Time unit for which to return the timeout.
     * @return Timeout.
     */
    default long getTimeout(TimeUnit unit) {
        return unit.convert(500, TimeUnit.MILLISECONDS);
    };

    /**
     * Maximum permitted pool size.
     * 
     * @return max-pool-size.
     */
    default int getMaxPoolSize() {
        return ((int) Math.round(Runtime.getRuntime().availableProcessors() * 1.5));
    };

    /**
     * Minimum permitted pool size.
     * 
     * @return core-pool-size.
     */
    default int getCorePoolSize() {
        return Runtime.getRuntime().availableProcessors();
    };
}