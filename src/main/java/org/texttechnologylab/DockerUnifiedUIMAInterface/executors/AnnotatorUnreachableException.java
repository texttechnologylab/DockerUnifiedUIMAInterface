package org.texttechnologylab.DockerUnifiedUIMAInterface.executors;

import org.texttechnologylab.DockerUnifiedUIMAInterface.executors.DUUIComponentParallelPipelineExecutor.DUUIWorker;

public class AnnotatorUnreachableException extends RuntimeException{

    public DUUIWorker failedWorker;
    final public boolean resuable;

    public AnnotatorUnreachableException(DUUIWorker worker, Exception e) {
        super(e);
        failedWorker = worker;
        resuable = false;
    }

    public AnnotatorUnreachableException(String format) {
        super(format);
        resuable = true;
    }

    public void setFailedWorker(DUUIWorker worker) {
        failedWorker = worker;
    }
    
}
