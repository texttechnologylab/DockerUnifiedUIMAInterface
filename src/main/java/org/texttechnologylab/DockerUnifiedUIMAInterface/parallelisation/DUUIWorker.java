package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static java.lang.String.format;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler.documentUpdate;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler.finalizeDocument;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler.updatePipelineGraphStatus;

import java.io.Serializable;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.jcas.JCas;
import org.texttechnologylab.DockerUnifiedUIMAInterface.AnnotatorUnreachableException;
import org.texttechnologylab.DockerUnifiedUIMAInterface.PipelinePart;
import org.texttechnologylab.DockerUnifiedUIMAInterface.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIParallelPipelineExecutor.PipelineWorker;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;

import guru.nidi.graphviz.attribute.Color;

public class DUUIWorker implements Callable<DUUIWorker>, Serializable, PipelineWorker {
    public final String _name;
    public final PipelinePart _component;
    public JCas _jc;
    public final DUUIPipelineDocumentPerformance _perf;
    public final Collection<DUUIWorker.ComponentLock> _parentLocks;
    public final Collection<DUUIWorker.ComponentLock> _childrenLocks;
    public final Signature _signature;
    public final String _threadName; 
    public final Set<String> _childrenIds;
    public final int _height;
    public final CountDownLatch _jCasLock;

    final Set<ComponentLock> _finishedParents = new HashSet<>();

    public DUUIWorker(String name, 
                PipelinePart component, 
                JCas jc, 
                DUUIPipelineDocumentPerformance perf, 
                Collection<DUUIWorker.ComponentLock> selfLatches, 
                Collection<DUUIWorker.ComponentLock> childLatches,
                Set<String> children,
                int height, 
                CountDownLatch jCasLock) {
        _name = name;
        _component = component; 
        _jc = jc; 
        _perf = perf; 
        _parentLocks = selfLatches; 
        _childrenLocks = childLatches; 
        _signature = _component.getSignature(); 
        _childrenIds = children;
        _height = height;
        _jCasLock = jCasLock;
        // _finishedParents = new HashSet<>(_parentLocks.size());

        _threadName = format("Worker-%s-%s", _name, _signature);
    }

    @Override
    public DUUIWorker call() throws Exception {
        Thread.currentThread().setName(_threadName);
        ResourceManager.register(Thread.currentThread(), true);
        long start = System.nanoTime();
        long parentWait = System.nanoTime();
        try {

            long parentWaitStart = System.nanoTime();
            while (_finishedParents.size() != _parentLocks.size()) {
                for (DUUIWorker.ComponentLock parent : _parentLocks) {
                    boolean finished = parent.await(1, TimeUnit.SECONDS);
                    if (parent.failed())
                        throw new RuntimeException(format("[%s] Parent failed.%n", _threadName));

                    if (finished) 
                        _finishedParents.add(parent);
                }
                if (Thread.interrupted()) 
                    throw new InterruptedException(format("[%s] interrupted.%n", _threadName));
            }
            parentWait = System.nanoTime() - parentWaitStart;
                        
            System.out.printf("[%s] starting analysis.%n", _threadName);
            updatePipelineGraphStatus(_name, _signature.toString(), Color.YELLOW2);
            _component.run(_name, _jc, _perf); 
            updatePipelineGraphStatus(_name, _signature.toString(), Color.GREEN3);
            System.out.printf("[%s] finished analysis.%n", _threadName);
                
            for (DUUIWorker.ComponentLock childLock : _childrenLocks)
                childLock.countDown(); // children can continue
                 
            } catch (AnnotatorUnreachableException e) {
                updatePipelineGraphStatus(_name, _signature.toString(), Color.RED3);
                for (DUUIWorker.ComponentLock childLock : _childrenLocks)
                    childLock.fail();
                e.setFailedWorker(this);
                throw e;
            } catch (Exception e) {
                updatePipelineGraphStatus(_name, _signature.toString(), Color.RED3);
                for (DUUIWorker.ComponentLock childLock : _childrenLocks)
                    childLock.fail();
                throw new RuntimeException(format(
                    "[%s] Pipeline component failed.%n", _threadName), e
                );
            } finally {
                documentUpdate(_name, _signature, "parent_wait", parentWait);
                documentUpdate(_name, _signature, "worker_total", System.nanoTime() - start);
                finalizeDocument(_name, _signature.toString());
                _jCasLock.countDown();
            }

        return this; 
    }



    public int getPriority() {
        return _height; // Height of 1 corresponds to root nodes
    }

    public static class ComponentLock extends CountDownLatch {

        AtomicBoolean failed = new AtomicBoolean(false);

        public ComponentLock(int count) {
            super(count);
        }

        public boolean failed() {
            return failed.get();
        }

        public void fail() {
            failed.set(true);
        }
    
    }
}