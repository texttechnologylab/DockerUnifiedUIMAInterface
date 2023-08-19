package org.texttechnologylab.DockerUnifiedUIMAInterface;

import java.util.concurrent.Callable;

public class AnnotatorUnreachableException extends RuntimeException{

    public Callable<?> failedWorker;

    public AnnotatorUnreachableException(String format) {
        super(format);
    }

    public void setFailedWorker(Callable<?> worker) {
        failedWorker = worker;
    }
    
}
