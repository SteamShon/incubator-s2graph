package org.apache.s2graph.core.tinkerpop.structure;

import org.apache.s2graph.core.mysqls.ColumnMeta;
import org.apache.tinkerpop.gremlin.structure.*;
import java.util.*;

public class S2Vertex implements Vertex {
    private S2Graph graph;
    private S2VertexId vertexId;
    private Map<String, VertexProperty<?>> props;
    private long ts;
    private String operation;

    public S2Vertex(S2Graph graph, S2VertexId vertexId) {
        this.graph = graph;
        this.vertexId = vertexId;
        this.props = new HashMap<>();
        this.ts = System.currentTimeMillis();
        this.operation = "insert";
    }

    // TODO: Need to add more constructor for handling property and ts and operaiton.
    public S2Vertex(S2Graph graph, org.apache.s2graph.core.Vertex vertex) {
        this.graph = graph;
        this.vertexId = new S2VertexId(vertex.service(), vertex.serviceColumn(), vertex.innerIdVal());
        this.ts = vertex.ts();
        this.operation = vertex.operation();
        this.props = new HashMap<>();
        for (Map.Entry<ColumnMeta, Object> e: scala.collection.JavaConversions.mapAsJavaMap(vertex.toProperties()).entrySet()) {
            ColumnMeta meta = e.getKey();
            property(VertexProperty.Cardinality.single, meta.name(), e.getValue());
        }
    }
    @Override
    public Edge addEdge(String s, Vertex vertex, Object... objects) {
        return null;
    }
    public Iterator<Edge> edgesAsync(Direction direction, String... edgeLabels) {
        return null;
    }
    @Override
    public Iterator<Edge> edges(Direction direction, String... strings) {
        return null;
    }
    @Override
    public Iterator<Vertex> vertices(Direction direction, String... edgeLabels) {
        List<Vertex> vertices = new ArrayList<>();
        Iterator<Edge> iter = edges(direction, edgeLabels);
        while (iter.hasNext()) {
            Edge edge = iter.next();
            if (direction == Direction.OUT) {
                vertices.add(edge.inVertex());
            } else if (direction == Direction.IN) {
                vertices.add(edge.outVertex());
            } else {
                vertices.add(edge.inVertex());
                vertices.add(edge.outVertex());
            }
        }
        return vertices.iterator();
    }



    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String key, V value, Object... keyValues) {
        if (cardinality == VertexProperty.Cardinality.single) {
            VertexProperty<V> newProperty = new S2VertexProperty<V>(this, key, value);
            props.put(key, newProperty);
            return newProperty;
        } else {
            throw new RuntimeException("only support single cardinalrity currently.");
        }
    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... keys) {
        return null;
    }

    @Override
    public Object id() {
        return vertexId;
    }

    @Override
    public String label() {
        return null;
    }

    @Override
    public Graph graph() {
        return graph;
    }

    @Override
    public void remove() {
        // to nothing.
    }


    public S2Graph getGraph() {
        return graph;
    }

    public void setGraph(S2Graph graph) {
        this.graph = graph;
    }

    public S2VertexId getVertexId() {
        return vertexId;
    }

    public void setVertexId(S2VertexId vertexId) {
        this.vertexId = vertexId;
    }

    public Map<String, VertexProperty<?>> getProps() {
        return props;
    }

    public void setProps(Map<String, VertexProperty<?>> props) {
        this.props = props;
    }

    public long getTs() {
        return ts;
    }

    public void setTs(long ts) {
        this.ts = ts;
    }

    public String getOperation() {
        return operation;
    }

    public void setOperation(String operation) {
        this.operation = operation;
    }
}
