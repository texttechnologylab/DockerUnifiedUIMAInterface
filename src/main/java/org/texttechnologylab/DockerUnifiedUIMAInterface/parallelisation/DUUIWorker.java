package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static java.lang.String.format;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler.documentUpdate;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler.finalizeDocument;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler.updatePipelineGraphStatus;

import java.time.Instant;
import java.util.Collection;
import java.util.HashSet;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.uima.fit.util.JCasUtil;
import org.apache.uima.jcas.JCas;
import org.javatuples.Pair;
import org.texttechnologylab.ResourceManager;
import org.texttechnologylab.DockerUnifiedUIMAInterface.PipelinePart;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import guru.nidi.graphviz.attribute.Color;

public class DUUIWorker implements Callable<Pair<String, Set<String>>> {
    final String _name;
    final PipelinePart _component;
    final JCas _jc;
    final DUUIPipelineDocumentPerformance _perf;
    final Collection<DUUIWorker.ComponentLock> _parentLocks;
    final Collection<DUUIWorker.ComponentLock> _childrenLocks;
    final Signature _signature;
    final String _threadName; 
    final Set<String> _childrenIds;
    final int _height;

    final Set<ComponentLock> _finishedParents = new HashSet<>();

    public DUUIWorker(String name, 
                PipelinePart component, 
                JCas jc, 
                DUUIPipelineDocumentPerformance perf, 
                Collection<DUUIWorker.ComponentLock> selfLatches, 
                Collection<DUUIWorker.ComponentLock> childLatches,
                Set<String> children,
                int height) {
        _name = name;
        _component = component; 
        _jc = jc; 
        _perf = perf; 
        _parentLocks = selfLatches; 
        _childrenLocks = childLatches; 
        _signature = _component.getSignature(); 
        _childrenIds = children;
        _height = height;
        // _finishedParents = new HashSet<>(_parentLocks.size());

        _threadName = format("Worker-%s-%s", _name, _signature);
    }

    @Override
    public Pair<String, Set<String>> call() throws Exception {
        Thread.currentThread().setName(_threadName);
        ResourceManager.register(Thread.currentThread());
        long start = System.nanoTime();
        long parentWait = System.nanoTime();
        try {

            long parentWaitStart = System.nanoTime();
            while (_finishedParents.size() != _parentLocks.size()) {
                for (DUUIWorker.ComponentLock parent : _parentLocks) {
                    boolean finished = parent.await(1, TimeUnit.SECONDS);
                    if (parent.failed())
                        throw new Exception(format("[%s] Parent failed.%n", _threadName));

                    if (finished) 
                        _finishedParents.add(parent);
                }
                if (Thread.interrupted()) 
                    throw new Exception(format("[%s] interrupted.%n", _threadName));
            }
            parentWait = System.nanoTime() - parentWaitStart;
                        
            System.out.printf("[%s] starting analysis.%n", _threadName);
            updatePipelineGraphStatus(_name, _signature.toString(), Color.YELLOW2);
            _component.run(_name, _jc, _perf); 
            updatePipelineGraphStatus(_name, _signature.toString(), Color.GREEN3);
            System.out.printf("[%s] finished analysis.%n", _threadName);
                
            for (DUUIWorker.ComponentLock childLock : _childrenLocks)
                childLock.countDown(); // children can continue
                    
            } catch (Exception e) {
                updatePipelineGraphStatus(_name, _signature.toString(), Color.RED3);
                for (DUUIWorker.ComponentLock childLock : _childrenLocks)
                    childLock.fail();
                throw new Exception(format("[%s] Pipeline component failed.%n", _threadName), e);
            } finally {
                documentUpdate(_name, _signature, "parent_wait", parentWait);
                documentUpdate(_name, _signature, "worker_total", System.nanoTime() - start);
                finalizeDocument(_name, _signature.toString());
            }

        return Pair.with(_name, _childrenIds); 
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