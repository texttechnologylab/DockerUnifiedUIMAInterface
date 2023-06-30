package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

import static java.lang.String.format;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIPipelineProfiler.documentUpdate;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIPipelineProfiler.updatePipelineGraphStatus;
import static org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation.DUUIPipelineProfiler.finalizeDocument;

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
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer.PipelinePart;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import guru.nidi.graphviz.attribute.Color;

public class DUUIWorker implements Callable<Boolean> {
    private final String name;
    private final PipelinePart component;
    private final JCas jc;
    private final DUUIPipelineDocumentPerformance perf;
    private final Collection<DUUIWorker.ComponentLock> parentLocks;
    private final Collection<DUUIWorker.ComponentLock> childrenLocks;
    private Set<ComponentLock> finishedParents = new HashSet<>();

    public DUUIWorker(String name, 
                PipelinePart component, 
                JCas jc, 
                DUUIPipelineDocumentPerformance perf, 
                Collection<DUUIWorker.ComponentLock> selfLatches, 
                Collection<DUUIWorker.ComponentLock> childLatches) {
        this.name = name;
        this.component = component; 
        this.jc = jc; 
        this.perf = perf; 
        this.parentLocks = selfLatches; 
        this.childrenLocks = childLatches; 
    }

    @Override
    public Boolean call() throws Exception {
        long start = 0;
        try {
            String title = JCasUtil.select(jc, DocumentMetaData.class)
                .stream().map(meta -> meta.getDocumentTitle()).findFirst().orElseGet(() -> "");
            DUUIPipelineProfiler.add(name, title, component.getSignature(), component.getScale());

            while (finishedParents.size() != parentLocks.size()) {
                for (DUUIWorker.ComponentLock parent : parentLocks) {
                    boolean finished = parent.await(1, TimeUnit.SECONDS);
                    if (parent.failed()) {
                        throw new Exception(format("[DUUIWorker-%s][%s][Component: %s] Parent failed.%n", 
                            Thread.currentThread().getName(), name, component.getSignature()));
                    }
                    if (finished) finishedParents.add(parent);
                }
                if (Thread.interrupted()) 
                    throw new Exception(format("[DUUIWorker-%s][%s][Component: %s] interrupted.%n", 
                            Thread.currentThread().getName(), name, component.getSignature()));
            }
            
            updatePipelineGraphStatus(name, component.getSignature().toString(), Color.YELLOW2);
            // System.out.printf(
            //     "[DUUIWorker-%s][%s] Pipeline component %s starting analysis.%n", 
            //     Thread.currentThread().getName(), name, component.getSignature()
            // );
            start = Instant.now().getEpochSecond();
            component.run(name, jc, perf); 
            updatePipelineGraphStatus(name, component.getSignature().toString(), Color.GREEN3);
            // System.out.printf(
            //     "[DUUIWorker-%s][%s] Pipeline component %s finished analysis.%n", 
            //     Thread.currentThread().getName(), name, component.getSignature()
            // );
                
            for (DUUIWorker.ComponentLock childLock : childrenLocks)
                childLock.countDown(); // children can continue
                    
            } catch (Exception e) {
                updatePipelineGraphStatus(name, component.getSignature().toString(), Color.RED3);
                for (DUUIWorker.ComponentLock childLock : childrenLocks)
                    childLock.fail();
                throw new Exception(format("[DUUIWorker-%s][%s] Pipeline component %s failed.%n",
                    Thread.currentThread().getName(), name, component.getSignature()
                ));
            } finally {
                Instant end = Instant.now().minusSeconds(start);
                documentUpdate(name, component.getSignature(), "total", end);
                finalizeDocument(name, component.getSignature().toString());
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