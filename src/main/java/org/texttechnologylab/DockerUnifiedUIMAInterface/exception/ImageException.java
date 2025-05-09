package org.texttechnologylab.DockerUnifiedUIMAInterface.exception;

public class ImageException extends Exception {

    public ImageException() {
    }

    public ImageException(Throwable pCause) {
        super(pCause);
    }

    public ImageException(String pMessage) {
        super(pMessage);
    }

    public ImageException(String pMessage, Throwable pCause) {
        super(pMessage, pCause);
    }

}
