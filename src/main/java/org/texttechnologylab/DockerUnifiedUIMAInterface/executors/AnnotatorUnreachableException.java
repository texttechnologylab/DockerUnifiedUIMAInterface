package org.texttechnologylab.DockerUnifiedUIMAInterface.executors;

import org.texttechnologylab.DockerUnifiedUIMAInterface.executors.DUUIComponentParallelPipelineExecutor.DUUITask;

/**
 * Exception used by {@link DUUIComponentParallelPipelineExecutor}s to reschedule tasks.
 * 
 */
public class AnnotatorUnreachableException extends RuntimeException{

    public DUUITask failedWorker;
    final public boolean resuable;

    public AnnotatorUnreachableException(DUUITask worker, Exception e) {
        super(e);
        failedWorker = worker;
        resuable = false;
    }

    public AnnotatorUnreachableException(String format) {
        super(format);
        resuable = true;
    }

    public void setFailedWorker(DUUITask worker) {
        failedWorker = worker;
    }
    
}
