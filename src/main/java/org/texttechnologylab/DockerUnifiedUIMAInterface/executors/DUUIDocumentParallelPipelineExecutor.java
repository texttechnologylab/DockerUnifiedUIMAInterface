package org.texttechnologylab.DockerUnifiedUIMAInterface.executors;

import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.Config;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager;

/**
 * Pipeline-Executor used to schedule the processing of CAS documents in a thread-pool.
 * One document is processed sequentially in a single thread. 
 */
public class DUUIDocumentParallelPipelineExecutor extends DUUILinearPipelineExecutor {

    final ThreadPoolExecutor _executor;

    public DUUIDocumentParallelPipelineExecutor(Vector<PipelinePart> instantiatedPipeline) {
        super(instantiatedPipeline);

        _executor = new ThreadPoolExecutor(
            Config.strategy().getMaxPoolSize(), 
            Config.strategy().getMaxPoolSize(), 
            Config.strategy().getTimeout(TimeUnit.MILLISECONDS), 
            TimeUnit.MILLISECONDS, 
            new LinkedBlockingQueue<>());

        _executor.setRejectedExecutionHandler((Runnable r, ThreadPoolExecutor executor) -> {
            executor.getQueue().add(r);
            if (executor.isShutdown()) {
                throw new RejectedExecutionException("Task " + r + " rejected from " + executor);
            }
        });
    }
    
    @Override
    public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {
        Callable<Void> runner = () -> {
            ResourceManager.register(Thread.currentThread());
            Thread.currentThread().setName(name);
            super.run(name, jc, perf);
            return null;
        };
        long start = System.nanoTime();
        _executor.submit(runner);
        DUUIComposer.totalafterworkerwait.getAndAdd(System.nanoTime() - start);
    }

    /**
     * Shutdown thread-pool.
     */
    @Override
    public void shutdown() throws Exception {
        _executor.shutdown();
        while (!_executor
            .awaitTermination(100, TimeUnit.MILLISECONDS))
        {}
    }

    
    /**
     * Shutdown thread-pool.
     */
    @Override
    public void destroy() {
        _executor.shutdownNow();
    }
}
