package org.texttechnologylab.DockerUnifiedUIMAInterface.exception;

public class CommunicationLayerException extends Exception {
    public CommunicationLayerException() {
        super();
    }
    public CommunicationLayerException(Throwable pCause) {
        super(pCause);
    }
    public CommunicationLayerException(String pMessage) {
        super(pMessage);
    }
    public CommunicationLayerException(String pMessage, Throwable pCause) {
        super(pMessage, pCause);
    }
}
