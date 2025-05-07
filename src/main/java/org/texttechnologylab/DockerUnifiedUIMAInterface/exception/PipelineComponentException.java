package org.texttechnologylab.DockerUnifiedUIMAInterface.exception;

import de.tudarmstadt.ukp.dkpro.core.api.metadata.type.DocumentMetaData;
import org.texttechnologylab.DockerUnifiedUIMAInterface.driver.DUUIPipelineComponent;

public class PipelineComponentException extends Exception {
    public PipelineComponentException() {
        super();
    }

    public PipelineComponentException(Throwable cause) {
        super(cause);
    }

    public PipelineComponentException(String message) {
        super(message);
    }

    public PipelineComponentException(String message, Throwable cause) {
        super(message, cause);
    }

    public PipelineComponentException(DUUIPipelineComponent component) {
        this(
                "Encountered an error in component %s".formatted(component.toJson())
        );
    }

    public PipelineComponentException(DUUIPipelineComponent component, Throwable cause) {
        this(
                "Encountered an error in component %s".formatted(component.toJson()),
                cause
        );
    }

    public PipelineComponentException(DUUIPipelineComponent component, DocumentMetaData documentMetaData) {
        this(
                "Encountered an error in component %s while processing %s".formatted(
                        component.toJson(),
                        documentMetaData.getDocumentId()
                )
        );
    }

    public PipelineComponentException(DUUIPipelineComponent component, DocumentMetaData documentMetaData, Throwable cause) {
        this(
                "Encountered an error in component %s while processing %s".formatted(
                        component.toJson(),
                        documentMetaData.getDocumentId()
                ),
                cause
        );
    }
}

