package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import org.texttechnologylab.DockerUnifiedUIMAInterface.DUUIComposer;
import org.texttechnologylab.DockerUnifiedUIMAInterface.pipeline_storage.DUUIPipelineDocumentPerformance;
public final class DUUILuaLogger {

    private volatile DUUIComposer composer;
    private volatile String componentKey;
    private volatile String componentName;
    private volatile String documentId;
    private volatile DUUIPipelineDocumentPerformance perf;

    /**
     * Bind the current logging context. Called by the communication layer right before a script is run for a given document.
     */
    public void bind(DUUIComposer composer, String componentKey, String componentName,
                     String documentId, DUUIPipelineDocumentPerformance perf) {
        this.composer = composer;
        this.componentKey = componentKey;
        this.componentName = componentName;
        this.documentId = documentId;
        this.perf = perf;
    }

    /** Clear the current context so stray later calls cannot route to a stale composer/document. */
    public void clear() {
        this.composer = null;
        this.componentKey = null;
        this.componentName = null;
        this.documentId = null;
        this.perf = null;
    }

    // --- Lua API

    public void logTrace(String message) {
        log("TRACE", null, message);
    }

    public void logDebug(String message) {
        log("DEBUG", null, message);
    }

    public void logInfo(String message) {
        log("INFO", null, message);
    }

    public void logWarn(String message) {
        log("WARN", null, message);
    }

    public void logError(String message) {
        log("ERROR", null, message);
    }

    public void logCritical(String message) {
        log("CRITICAL", null, message);
    }

    // Overloads taking an explicit logger name, e.g. DUUILogging:logInfo("myScript", "...").

    public void logTrace(String logger, String message) {
        log("TRACE", logger, message);
    }

    public void logDebug(String logger, String message) {
        log("DEBUG", logger, message);
    }

    public void logInfo(String logger, String message) {
        log("INFO", logger, message);
    }

    public void logWarn(String logger, String message) {
        log("WARN", logger, message);
    }

    public void logError(String logger, String message) {
        log("ERROR", logger, message);
    }

    public void logCritical(String logger, String message) {
        log("CRITICAL", logger, message);
    }

    public void log(String level, String logger, String message) {
        log(level, logger, message, null);
    }

    public void log(String level, String logger, String message, String stacktrace) {
        DUUIComposer c = this.composer;
        if (c == null) {
            return;
        }
        DUUIComponentLog.record(
                c, componentKey, componentName, documentId,
                level, logger, message, stacktrace, System.currentTimeMillis(), perf);
    }
}
