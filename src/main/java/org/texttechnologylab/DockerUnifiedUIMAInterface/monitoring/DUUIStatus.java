package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

public class DUUIStatus {
    public static final String ACTIVE = "Active";
    public static final String ANY = "Any";
    public static final String CANCELLED = "Cancelled";
    public static final String COMPLETED = "Completed";
    public static final String DECODE = "Decode";
    public static final String DESERIALIZE = "Deserialize";
    public static final String DOWNLOAD = "Download";
    public static final String FAILED = "Failed";
    public static final String IDLE = "Idle";
    public static final String INACTIVE = "Inactive";
    public static final String INPUT = "Input";
    public static final String INSTANTIATING = "Instantiating";
    public static final String OUTPUT = "Output";
    public static final String SETUP = "Setup";
    public static final String SHUTDOWN = "Shutdown";
    public static final String SKIPPED = "Skipped";
    public static final String IMAGE_START = "Starting";
    public static final String UNKNOWN = "Unknown";
    public static final String WAITING = "Waiting";

    /**
     * Checks wether the given status is any of the Status names provided as options.
     *
     * @param status  The status to look for.
     * @param options The valid options the status can be.
     * @return If the status is part of options.
     */
    public static boolean oneOf(String status, String... options) {
        for (String option : options) {
            if (status.equalsIgnoreCase(option)) {
                return true;
            }
        }

        return false;
    }
}