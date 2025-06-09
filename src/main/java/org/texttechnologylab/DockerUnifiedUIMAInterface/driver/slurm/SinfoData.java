package org.texttechnologylab.DockerUnifiedUIMAInterface.driver.slurm;

import com.fasterxml.jackson.annotation.JsonSetter;
import com.fasterxml.jackson.annotation.Nulls;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.util.List;

/**
 * cli insruction sinfo  in java console
 * für zukunftige DEBUG und log
 */
public class SinfoData {

    public static class SinfoResponse {
        public List<SinfoRecord> sinfo = List.of();
        public Meta meta = new Meta();
        public List<String> errors = List.of();
        public List<String> warnings = List.of();
    }

    public static class SinfoRecord {
        public int port;
        public Nodes nodes = new Nodes();
        public Cpus cpus = new Cpus();
        public Node node = new Node();
        public Gres gres = new Gres();
        public Memory memory = new Memory();
        public Partition partition = new Partition();

        public String comment = "", cluster = "", extra = "", reservation = "";
    }


    public static class Node {
        public List<String> state = List.of();
    }

    public static class Nodes {
        public int total, idle, allocated, other;
        public List<String> hostnames = List.of(), addresses = List.of(), nodes = List.of();
    }

    public static class Cpus {
        public int total, idle, allocated, other;
    }

    public static class Gres {
        public String total = "", used = "";
    }

    public static class Memory {
        public int minimum, maximum, allocated;
        public Free free = new Free();

        public static class Free {
            public NumberObj minimum = new NumberObj(), maximum = new NumberObj();

            public static class NumberObj {
                public boolean set;
                public boolean infinite;
                public int number;
            }
        }
    }

    public static class Partition {
        public String name = "";
        public PartitionState partition = new PartitionState();

        public static class PartitionState {
            public List<String> state = List.of();
        }
    }

    public static class Meta {
        public Slurm slurm = new Slurm();

        public static class Slurm {
            public String release = "";
        }
    }


    private static SinfoResponse runSinfo() {
        StringBuilder json = new StringBuilder();
        try {
            Process p = new ProcessBuilder("sinfo", "--json").redirectErrorStream(true).start();
            try (BufferedReader br = new BufferedReader(new InputStreamReader(p.getInputStream()))) {
                br.lines().forEach(json::append);
            }
            p.waitFor();
        } catch (Exception e) {
            throw new RuntimeException("sinfo failed", e);
        }

        ObjectMapper mapper = new ObjectMapper()

                .setDefaultSetterInfo(JsonSetter.Value.forValueNulls(Nulls.AS_EMPTY))

                .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

        try {
            return mapper.readValue(json.toString(), SinfoResponse.class);
        } catch (Exception e) {
            throw new RuntimeException("parse json fail...", e);
        }
    }

    public static void sinfo() {
        SinfoResponse resp = runSinfo();

        String hdrFmt = "%-10s %10s %6s %10s %10s %-10s %-10s %11s %9s %11s%n";
        String rowFmt = "%-10s %6s %8s %10s %10s %-12s %-10s %9d %8d %10d%n";

        System.out.printf(hdrFmt,
                "Partition", "NodeTotal", "Idle", "CPU_Total", "CPU_Idle",
                "State", "GRES", "MemTot(MB)", "MemAlloc", "MemFreeMin");
        System.out.println("--------------------------------------------------------------------------------------------------------");

        for (SinfoRecord r : resp.sinfo) {
            System.out.printf(rowFmt,
                    r.partition.name,
                    r.nodes.total,
                    r.nodes.idle,
                    r.cpus.total,
                    r.cpus.idle,
                    r.node.state,
                    r.gres.total,
                    r.memory.maximum,
                    r.memory.allocated,
                    r.memory.free.minimum.number);
        }
        System.out.println("--------------------------------------------------------------------------------------------------------");
        System.out.printf("Slurm release: %s  |  Errors: %s%n",
                resp.meta.slurm.release,
                resp.errors);
    }




}



