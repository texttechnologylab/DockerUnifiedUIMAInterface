package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static java.lang.String.format;

import java.time.Duration;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIParallelPipelineExecutor.PipelineWorker;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.ResourceManager;

public class DUUIWorker implements Callable<DUUIWorker>, PipelineWorker {
    public final String _name;
    public final PipelinePart _component;
    public final JCas _jc;
    public final DUUIPipelineDocumentPerformance _perf;
    public final Signature _signature;
    public final String _threadName; 
    public final Set<String> _childrenIds;
    public final int _height;
    public final CountDownLatch _jCasLock;
    Duration _total = Duration.ofSeconds(0);

    public DUUIWorker(String name, 
                PipelinePart component, 
                JCas jc, 
                DUUIPipelineDocumentPerformance perf,
                Set<String> children,
                int height, 
                CountDownLatch jCasLock) {
        _name = name;
        _component = component; 
        _jc = jc; 
        _perf = perf;
        _signature = _component.getSignature(); 
        _childrenIds = children;
        _height = height;
        _jCasLock = jCasLock;

        _threadName = format("%s-%s", _name, _signature);
    }

    @Override
    public DUUIWorker call() throws Exception {
        Thread.currentThread().setName(_threadName);
        ResourceManager.register(Thread.currentThread(), true);
        try {
            // System.out.printf("[%s] starting analysis.%n", _threadName);
            _component.run(_name, _jc, _perf); 
            // System.out.printf("[%s] finished analysis.%n", _threadName);
                 
        } catch (AnnotatorUnreachableException e) { 
            e.setFailedWorker(this); // Task might be rescheduled
            throw e;
        } catch (Exception e) { // Task won't be rescheduled
            throw new AnnotatorUnreachableException(this, 
                new RuntimeException(format("[%s] Pipeline component failed.%n", _threadName), e));
        }
        return this; 
    }

    public Duration total() {
        return _total;
    }

    public int getPriority() {
        return _height;
    }

    public String component() {
        return _component.getUUID();
    }


    public String toString() {
        return "Component="+_signature+", Document"+_name;
    }
}