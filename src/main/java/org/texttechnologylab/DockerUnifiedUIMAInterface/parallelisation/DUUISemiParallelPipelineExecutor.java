package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import java.util.Vector;
import java.util.concurrent.Callable;
import java.util.concurrent.LinkedBlockingDeque;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.PipelinePart;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;

public class DUUISemiParallelPipelineExecutor extends DUUILinearPipelineExecutor {

    final class RejectingQueue extends LinkedBlockingQueue<Runnable> {

        static final long serialVersionUID = -6903933921423432194L;
        @Override
        public boolean offer(Runnable e) {
            if (_executor.getPoolSize() >= DUUIComposer.Config.strategy().getMaxPoolSize()) {
                return super.offer(e);
            } else {
                // System.out.printf("[ParallelPipeline] NEW THREAD Pool: %d | Max: %d%n", 
                //     _executor.getPoolSize(), DUUIComposer.Config.strategy().getMaxPoolSize());
                return false;
            }
        }
    }

    final ThreadPoolExecutor _executor;

    public DUUISemiParallelPipelineExecutor(Vector<PipelinePart> instantiatedPipeline) {
        super(instantiatedPipeline);

        _executor = new ThreadPoolExecutor(
            DUUIComposer.Config.strategy().getCorePoolSize(), 
            DUUIComposer.Config.strategy().getMaxPoolSize(), 
            DUUIComposer.Config.strategy().getTimeout(TimeUnit.MILLISECONDS), 
            TimeUnit.MILLISECONDS, 
            new LinkedBlockingDeque<>());

        _executor.setRejectedExecutionHandler((Runnable r, ThreadPoolExecutor executor) -> {
            // System.out.println("NEW TASK " + r);
            executor.getQueue().add(r);
            if (executor.isShutdown()) {
                throw new RejectedExecutionException(
                    "Task " + r + " rejected from " + executor);
            }
        });
    }
    
    @Override
    public void run(String name, JCas jc, DUUIPipelineDocumentPerformance perf) {
        Callable<Void> runner = () -> {
            super.run(name, jc, perf);
            ResourceManager.getInstance().returnCas(jc);
            return null;
        };

        _executor.submit(runner);
    }

    @Override
    public void shutdown() throws Exception {
        _executor.shutdown();
        while (!_executor
            .awaitTermination(100, TimeUnit.MILLISECONDS))
        {}
    }

    @Override
    public void destroy() {
        _executor.shutdownNow();
    }
}
