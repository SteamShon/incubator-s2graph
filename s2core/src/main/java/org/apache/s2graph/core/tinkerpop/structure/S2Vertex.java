package org.apache.s2graph.core.tinkerpop.structure;

import org.apache.tinkerpop.gremlin.structure.*;

import java.util.Iterator;

public class S2Vertex implements Vertex {
    @Override
    public Edge addEdge(String s, Vertex vertex, Object... objects) {
        return null;
    }

    @Override
    public <V> VertexProperty<V> property(VertexProperty.Cardinality cardinality, String s, V v, Object... objects) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Direction direction, String... strings) {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Direction direction, String... strings) {
        return null;
    }

    @Override
    public Object id() {
        return null;
    }

    @Override
    public String label() {
        return null;
    }

    @Override
    public Graph graph() {
        return null;
    }

    @Override
    public void remove() {

    }

    @Override
    public <V> Iterator<VertexProperty<V>> properties(String... strings) {
        return null;
    }
}
