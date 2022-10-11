package org.texttechnologylab.DockerUnifiedUIMAInterface.parallelPlan;

import org.jgrapht.Graph;
import org.jgrapht.graph.DefaultDirectedGraph;
import org.jgrapht.graph.DefaultEdge;
import org.jgrapht.nio.Attribute;
import org.jgrapht.nio.AttributeType;
import org.jgrapht.nio.DefaultAttribute;
import org.jgrapht.nio.graphml.GraphMLExporter;

import java.io.File;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.Queue;

public class PlanToGraph {

    /**
     * generates a jgrapht graph from an DUUIParallelExecutionPlan
     */
    public static Graph<DUUIParallelExecutionPlan, DefaultEdge> toGraph(DUUIParallelExecutionPlan root) {
        Graph<DUUIParallelExecutionPlan, DefaultEdge> g = new DefaultDirectedGraph<>(DefaultEdge.class);
        Queue<DUUIParallelExecutionPlan> queue = new LinkedList<>();
        queue.add(root);
        g.addVertex(root);
        while (!queue.isEmpty()) {
            DUUIParallelExecutionPlan plan = queue.poll();
            for (DUUIParallelExecutionPlan next : plan.getNext()) {
                g.addVertex(next);
                g.addEdge(plan, next);
                queue.add(next);
            }
        }
        return g;
    }

    /**
     * exports a graph
     */
    public static void writeGraph(Graph<DUUIParallelExecutionPlan, DefaultEdge> graph, String fileName) {
        GraphMLExporter<DUUIParallelExecutionPlan, DefaultEdge> exporter = new GraphMLExporter<>();
        exporter.registerAttribute("inputs", GraphMLExporter.AttributeCategory.NODE, AttributeType.STRING);
        exporter.registerAttribute("outputs", GraphMLExporter.AttributeCategory.NODE, AttributeType.STRING);
        exporter.registerAttribute("uuid", GraphMLExporter.AttributeCategory.NODE, AttributeType.STRING);
        exporter.setVertexAttributeProvider(vertex -> {
            Map<String, Attribute> m = new HashMap<>();
            m.put("inputs", new DefaultAttribute<>(vertex.getPipelinePart() != null ? String.valueOf(vertex.getInputs()) : "null", AttributeType.STRING));
            m.put("outputs", new DefaultAttribute<>(vertex.getPipelinePart() != null ? String.valueOf(vertex.getOutputs()) : "null", AttributeType.STRING));
            m.put("uuid", new DefaultAttribute<>(vertex.getPipelinePart() != null ? vertex.getPipelinePart().getUUID() : "null", AttributeType.STRING));
            return m;
        });
        exporter.exportGraph(graph, new File(fileName));
    }

    /**
     * test only
     */
    public static void main(String[] args) {
        Graph<String, DefaultEdge> g = new DefaultDirectedGraph<>(DefaultEdge.class);
        g.addVertex("root");
        // g.getType();

        System.out.println(g);

        GraphMLExporter<String, DefaultEdge> exporter = new GraphMLExporter<>();
        exporter.registerAttribute("name", GraphMLExporter.AttributeCategory.NODE, AttributeType.STRING);
        exporter.setVertexAttributeProvider(vertex -> {
            Map<String, Attribute> m = new HashMap<>();
            m.put("name", new DefaultAttribute<>(vertex, AttributeType.STRING));
            return m;
        });
        exporter.exportGraph(g, new File("graph.graphml"));
    }


}
