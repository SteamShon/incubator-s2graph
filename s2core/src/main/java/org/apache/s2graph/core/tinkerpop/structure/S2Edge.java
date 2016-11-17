package org.apache.s2graph.core.tinkerpop.structure;

import org.apache.s2graph.core.GraphUtil;
import org.apache.tinkerpop.gremlin.structure.*;

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

public class S2Edge implements Edge {

    private S2Graph graph;
    private S2Vertex srcV;
    private S2Vertex tgtV;
    private String label;
    private String direction;
    private Map<String, Property<?>> props;
    private Long ts;
    private String operation;

    public S2Edge(S2Graph graph, org.apache.s2graph.core.Edge edge) {
        this(graph,
                new S2Vertex(graph, edge.srcVertex()),
                new S2Vertex(graph, edge.tgtVertex()),
                edge.labelName());
    }

    public S2Edge(S2Graph graph,
                  S2Vertex srcV,
                  S2Vertex tgtV,
                  String label) {
        this.graph = graph;
        this.srcV = srcV;
        this.tgtV = tgtV;
        this.label = label;
        this.direction = "out";
        this.ts = System.currentTimeMillis();
        this.operation = GraphUtil.defaultOp();
        this.props = new HashMap<>();
    }

    public S2Edge(S2Graph graph,
                  S2Vertex srcV,
                  S2Vertex tgtV,
                  String label,
                  String direction,
                  Map<String, Object> props,
                  Long ts,
                  String operation) {
        this.graph = graph;
        this.srcV = srcV;
        this.tgtV = tgtV;
        this.label = label;
        this.direction = direction;
        this.ts = ts;
        this.operation = operation;
        this.props = new HashMap<>();

        for (Map.Entry<String, Object> e : props.entrySet()) {
            property(e.getKey(), e.getValue());
        }
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction) {
        return null;
    }

    @Override
    public Object id() {
        return null;
    }

    @Override
    public String label() {
        return label;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public <V> Property<V> property(String key, V value) {
        Property<V> newProperty = new S2Property<V>(this, key, value);
        props.put(key, newProperty);
        return newProperty;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<Property<V>> properties(String... strings) {
        return null;
    }

    public S2Vertex srcV() {
        return srcV;
    }

    public S2Vertex tgtV() {
        return tgtV;
    }

    public String direction() {
        return direction;
    }

    public String operation() {
        return operation;
    }

    public Long ts() {
        return ts;
    }

    public Map<String, Property<?>> getProps() {
        return props;
    }

    @Override
    public String toString() {
        return "S2Edge{" +
                "graph=" + graph +
                ", srcV=" + srcV +
                ", tgtV=" + tgtV +
                ", label='" + label + '\'' +
                ", direction='" + direction + '\'' +
                ", props=" + props +
                ", ts=" + ts +
                ", operation='" + operation + '\'' +
                '}';
    }
}
