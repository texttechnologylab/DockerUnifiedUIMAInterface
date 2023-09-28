package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelisation;

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
