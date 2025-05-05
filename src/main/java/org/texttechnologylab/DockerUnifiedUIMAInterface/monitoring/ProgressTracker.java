package org.texttechnologylab.DockerUnifiedUIMAInterface.monitoring;

import org.apache.uima.util.Progress;

public class ProgressTracker implements Progress {
        private long completed;
        private long total;
        private String unit;
        private final boolean approximate;

        public ProgressTracker(long total, String unit, boolean approximate) {
            this.setTotal(total);
            this.completed = 0;
            this.unit = unit;
            this.approximate = approximate;
        }

        public ProgressTracker(long total, String unit) {
            this(total, unit, false);
        }

        public ProgressTracker(long total) {
            this(total, "items", false);
        }

        @Override
        public long getCompleted() {
            return this.completed;
        }

        public void setTotal(long value) throws IllegalArgumentException {
            if (value <= 0) {
                throw new IllegalArgumentException("Total must be > 0!");
            }
            this.total = value;
        }

        public void setCompleted(long value) throws IllegalArgumentException {
            if (value < 0 || value > getTotal()) {
                throw new IllegalArgumentException("completed must be >= 0 and <= total!");
            }
            this.completed = value;
        }

        @Override
        public long getTotal() {
            return this.total;
        }

        @Override
        public String getUnit() {
            return this.unit;
        }

        public void setUnit(String value) {
            this.unit = value;
        }

        @Override
        public boolean isApproximate() {
            return this.approximate;
        }

        public void inc() {
            this.completed += 1;
        }

        public void increment(long by) {
            this.completed += by;
        }

        @Override
        public String toString() {
            String format = String.format("% 4.0f%% [%s/%d %s]", getCompleted() * 100f / getTotal(), getCompletedF(), getTotal(), getUnit());
            if (isApproximate()) {
                format += " (approximate)";
            }
            return format;
        }

        protected String getCompletedF() {
            int digits = 1 + (int) Math.log10(getTotal());
            String format = "%" + digits + "d";
            return String.format(format, getCompleted());
        }
    }