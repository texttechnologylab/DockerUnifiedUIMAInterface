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
import org.texttechnologylab.DockerUnifiedUIMAInterface.PipelinePart;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.Signature;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineProfiler;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import guru.nidi.graphviz.attribute.Color;

public class DUUIWorker implements Callable<Boolean> {
    final String _name;
    final PipelinePart _component;
    final JCas _jc;
    final DUUIPipelineDocumentPerformance _perf;
    final Collection<DUUIWorker.ComponentLock> _parentLocks;
    final Collection<DUUIWorker.ComponentLock> _childrenLocks;
    final Signature _signature; 
    final String _title; 
    final String _threadName; 

    Set<ComponentLock> finishedParents = new HashSet<>();

    public DUUIWorker(String name, 
                PipelinePart component, 
                JCas jc, 
                DUUIPipelineDocumentPerformance perf, 
                Collection<DUUIWorker.ComponentLock> selfLatches, 
                Collection<DUUIWorker.ComponentLock> childLatches) {
        _name = name;
        _component = component; 
        _jc = jc; 
        _perf = perf; 
        _parentLocks = selfLatches; 
        _childrenLocks = childLatches; 
        _signature = _component.getSignature(); 
        _title = JCasUtil.select(_jc, DocumentMetaData.class)
                .stream().map(meta -> meta.getDocumentTitle()).findFirst().orElseGet(() -> "");

        _threadName = format("Worker-%s-%s", _name, _signature);
    }

    @Override
    public Boolean call() throws Exception {
        Thread.currentThread().setName(_threadName);
        long start = 0;
        try {
            
            DUUIPipelineProfiler.add(_name, _title, _signature, _component.getScale());

            while (finishedParents.size() != _parentLocks.size()) {
                for (DUUIWorker.ComponentLock parent : _parentLocks) {
                    boolean finished = parent.await(1, TimeUnit.SECONDS);
                    if (parent.failed())
                        throw new Exception(format("[%s] Parent failed.%n", _threadName));

                    if (finished) 
                        finishedParents.add(parent);
                }
                if (Thread.interrupted()) 
                    throw new Exception(format("[%s] interrupted.%n", _threadName));
            }
            
            System.out.printf("[%s] starting analysis.%n", _threadName);
            updatePipelineGraphStatus(_name, _signature.toString(), Color.YELLOW2);
            start = Instant.now().getEpochSecond();
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
                Instant end = Instant.now().minusSeconds(start);
                documentUpdate(_name, _signature, "total", end);
                finalizeDocument(_name, _signature.toString());
            }

        return Boolean.valueOf(true); 
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