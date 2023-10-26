package org.texttechnologylab.DockerUnifiedUIMAInterface.profiling.visualisation;

import static guru.nidi.graphviz.model.Factory.graphAttrs;
import static guru.nidi.graphviz.model.Factory.linkAttrs;
import static guru.nidi.graphviz.model.Factory.mutGraph;
import static guru.nidi.graphviz.model.Factory.node;
import static java.lang.String.format;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.util.Base64;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.commons.io.FileUtils;

import guru.nidi.graphviz.attribute.Color;
import guru.nidi.graphviz.attribute.Rank;
import guru.nidi.graphviz.attribute.Rank.RankDir;
import guru.nidi.graphviz.attribute.Style;
import guru.nidi.graphviz.engine.Format;
import guru.nidi.graphviz.engine.Graphviz;
import guru.nidi.graphviz.model.MutableGraph;
import guru.nidi.graphviz.model.Node;

public class DUUIPipelineVisualizer {
    
    final Map<String, Integer[]> progress = new HashMap<>();
    final Map<String, Integer> translate = new HashMap<>();
    final Map<String, Set<String>> pipeline;
    final MutableGraph graph;

    static final ByteArrayOutputStream high = new ByteArrayOutputStream(1024*1024*5);
    static final ByteArrayOutputStream low = new ByteArrayOutputStream(1024*50);

    public DUUIPipelineVisualizer(Map<String, Set<String>> pipeline) {
        this.pipeline = pipeline;
        graph = mutGraph().setDirected(true).use((gr, ctx) -> {
                graphAttrs().add(Rank.dir(RankDir.TOP_TO_BOTTOM));
                linkAttrs().add("class", "link-class");
            });

        int i = 0;
        for (String vertex : pipeline.keySet()) {
            Node parent = node(vertex);
            graph.add(parent);
            for (String child : pipeline.get(vertex)) {
                graph.add(parent.link(node(child)));
            }
            translate.put(vertex, i);
            i++;
        }
    }

    void initProgress(String name) {
        final Integer[] prog = new Integer[pipeline.size()]; 
        for (int j = 0; j < prog.length; j++) {
            prog[j] = 0;
        }
        progress.put(name, prog);
    }

    void removeIfFinished(String name) {
        final Integer[] prog = progress.get(name);
        for (int j = 0; j < prog.length; j++) {
            final boolean finished = prog[j] > 1; // TODO: Bug, if 3 and Task is rescheduled.
            if (! finished) return;
        }
        progress.remove(name);
    }

    Integer[] updateProgress(String name, String signature, Color color) {

        Integer[] prog = this.progress.getOrDefault(name, null);
        if (prog == null) initProgress(name);
        prog = this.progress.get(name);
        prog[translate.get(signature)] = translateColor(color);

        return prog;
    }

    public String updateGraph(String name, String signature, Color progress) {
        
        final Integer[] prog = updateProgress(name, signature, progress);

        graph.nodes().forEach(comp -> {
            final int i = translate.get(comp.name().value());
            final int state = prog[i];
            final Color color = translateState(state);
            comp.add(color, Style.lineWidth(2), Style.RADIAL);
        });
        
        final String png = writeToFile(name, false);

        removeIfFinished(name);

        return png;
    }

    public synchronized String writeToFile(String name, boolean highQuality) {
        ByteArrayOutputStream out = null;
        try {
            int width = 500; int height = 300;
            if (highQuality) {
                width = 1500; height = 1000; 
                out = high; // 5 MB for high quality pic
            } else {
                out = low; // 50 KB for low quality pic
            }
        
            Graphviz.fromGraph(graph)
                    .width(width)
                    .height(height)
                    .render(Format.PNG)
                    .toFile(new File(format("./Pipeline-Graph/%s.png", name)));
            // System.out.printf("[DUUIExecutionPipeline] Generated execution graph:
            // ./Execution-Pipeline/%s.png%n", name);
            return Base64.getEncoder().encodeToString(((ByteArrayOutputStream) out).toByteArray());
        } catch (Exception e) {
            System.out.println(format("[%s] Writing pipeline-graph to file failed: %n", format("./Pipeline-Graph/%s.png", name)));
            e.printStackTrace();
            return "";
        } finally {
            if (out != null) out.reset();
        }
    }
    
    int translateColor(Color progress) {
        
        if (progress == Color.YELLOW2) {
            return 1;
        } else if (progress == Color.GREEN3) {
            return 2;
        } else if (progress == Color.RED3) {
            return 3;
        } else {
            return 0;
        }
    }

    Color translateState(int state) {
        switch (state) {
            case 1:
                return Color.YELLOW2;
            case 2: 
                return Color.GREEN3;
            case 3:
                return Color.RED3;
            default:
                return Color.WHITE;
        }
    }

    public static String formatp(double percentage) {
        return format("%.2f%%", percentage*100.f);
    }

    public static String formatb(long bytes) {
        return FileUtils.byteCountToDisplaySize(bytes).replace(" ", "");
    }

    public static String formatns(long timeNS) {
        return timeNS >= 1000 ? 
            formatms(TimeUnit.MILLISECONDS.convert(timeNS, TimeUnit.NANOSECONDS)) : timeNS + "ns";
    }

    public static String formatms(long timeMS) {
        return timeMS >= 1000 ? 
            formatsec(TimeUnit.SECONDS.convert(timeMS, TimeUnit.MILLISECONDS)) + " " + (timeMS%1000 + "ms") : timeMS + "ms";
    }

    public static String formatsec(long timeSec) {
        return timeSec >= 60 ? 
            formatmin(TimeUnit.MINUTES.convert(timeSec, TimeUnit.SECONDS)) + " " + (timeSec%60 + "s") : timeSec + "s";
    }

    public static String formatmin(long timeMin) {
        return timeMin + "min";
    }
    
}