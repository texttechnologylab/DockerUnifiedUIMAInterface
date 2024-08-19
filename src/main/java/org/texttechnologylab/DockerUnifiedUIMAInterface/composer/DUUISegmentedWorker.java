package org.texttechnologylab.DockerUnifiedUIMAInterface.composer;

import org.apache.uima.UIMAException;
import org.apache.uima.fit.factory.JCasFactory;
import org.apache.uima.jcas.JCas;
import org.apache.uima.resource.metadata.TypeSystemDescription;
import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.io.DUUICollectionDBReader;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.IDUUIStorageBackend;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class DUUISegmentedWorker implements Runnable {
    private final int threadIndex;
    private final AtomicBoolean shutdown;
    private final DUUIComposer.PipelinePart pipelinePart;
    private final DUUICollectionDBReader collectionReader;
    private final TypeSystemDescription typesystem;
    private final IDUUIStorageBackend backend;
    private final String name;
    private final List<String> pipelineUUIDs;
    private final int pipelinePosition;

    public DUUISegmentedWorker(int threadIndex, AtomicBoolean shutdown, DUUIComposer.PipelinePart pipelinePart, DUUICollectionDBReader collectionReader, TypeSystemDescription typesystem, IDUUIStorageBackend backend, String name, List<String> pipelineUUIDs) {
        this.threadIndex = threadIndex;
        this.shutdown = shutdown;
        this.pipelinePart = pipelinePart;
        this.collectionReader = collectionReader;
        this.typesystem = typesystem;
        this.backend = backend;
        this.name = name;
        this.pipelineUUIDs = pipelineUUIDs;
        this.pipelinePosition = pipelineUUIDs.indexOf(pipelinePart.getUUID());
    }

    @Override
    public void run() {
        JCas jCas;
        try {
            jCas = JCasFactory.createJCas(typesystem);
        } catch (UIMAException e) {
            throw new RuntimeException(e);
        }

        boolean trackErrorDocs = false;
        if (backend != null) {
            trackErrorDocs = backend.shouldTrackErrorDocs();
        }

        while (true) {
            long waitTimeStart = System.nanoTime();
            while (true) {
                if (shutdown.get()) {
                    jCas.reset();
                    return;
                }
                try {
                    if (!collectionReader.getNextCas(jCas, pipelinePart.getUUID(), pipelinePosition)) {
                        Thread.sleep(300);
                    } else {
                        break;
                    }
                }
                catch (Exception e) {
                    e.printStackTrace();
                }
            }
            long waitTimeEnd = System.nanoTime();

            boolean status = false;
            try {
                DUUIPipelineDocumentPerformance perf = new DUUIPipelineDocumentPerformance(name, waitTimeEnd - waitTimeStart, jCas, trackErrorDocs);

                pipelinePart.getDriver().run(pipelinePart.getUUID(), jCas, perf, null);
                // TODO!!!! @Daniel
                //pipelinePart.getDriver().run(pipelinePart.getUUID(), jCas, perf);


                if (backend != null) {
                    backend.addMetricsForDocument(perf);
                }

                status = true;

            } catch (Exception e) {
                status = false;
                e.printStackTrace();
                System.err.println(e.getMessage());
                System.err.println("Error in pipeline part " + pipelinePart.getUUID() + ", continuing with next document!");
            }
            finally {
                collectionReader.updateCas(jCas, pipelinePart.getUUID(), status, pipelineUUIDs);
            }
        }
    }
}
