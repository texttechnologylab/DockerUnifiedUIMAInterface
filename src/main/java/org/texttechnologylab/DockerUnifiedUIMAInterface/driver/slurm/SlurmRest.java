package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

import org.json.JSONObject;

import java.io.IOException;

public interface SlurmRest {
    public abstract String showHostName() throws IOException;

    public abstract String query(String where);

    public abstract boolean cancelJob(String jobID) throws IOException;

    public abstract String[] submit(JSONObject params) throws IOException;


    public static class SlurmRestException extends RuntimeException {
        public SlurmRestException(String message) {
            super(message);
        }

        public SlurmRestException(String message, Throwable cause) {
            super(message, cause);
        }

    }
}
