package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import java.time.Duration;

public class ProgressTrackerRemaining extends ProgressTracker {
        private final long start = System.currentTimeMillis();

        public ProgressTrackerRemaining(long total, String unit, boolean approximate) {
            super(total, unit, approximate);
        }

        public ProgressTrackerRemaining(long total, String unit) {
            super(total, unit, false);
        }

        public ProgressTrackerRemaining(long total) {
            super(total, "items", false);
        }

        @Override
        public String toString() {
            long total = getTotal();
            long completed = getCompleted();
            double millisPassed = System.currentTimeMillis() - start * 1d;
            double millisEstimated = millisPassed / completed * total;
            long millisRemaining = (long) (millisEstimated - millisPassed);

            String remaining;
            Duration durationRemaining = Duration.ofMillis(millisRemaining);
            if (durationRemaining.toHours() > 0) {
                long hours = durationRemaining.toHours();
                long minutes = durationRemaining.minusHours(hours).toMinutes();
                long seconds = durationRemaining.minusHours(hours).minusMinutes(minutes).toSeconds();
                remaining = String.format("%dh %2dm %2ds", hours, minutes, seconds);
            } else if (durationRemaining.toMinutes() > 0) {
                long minutes = durationRemaining.toMinutes();
                long seconds = durationRemaining.minusMinutes(minutes).toSeconds();
                remaining = String.format("%dm %2ds", minutes, seconds);
            } else {
                remaining = String.format("%2ds", durationRemaining.toSeconds());
            }

            String format = String.format("% 4.0f%% [%s/%d %s, %s]", completed * 100f / total, getCompletedF(), total, getUnit(), remaining);
            if (isApproximate()) {
                format += " (approximate)";
            }
            return format;
        }
    }