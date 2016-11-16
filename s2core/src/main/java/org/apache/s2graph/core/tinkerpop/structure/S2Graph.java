package org.apache.s2graph.core.tinkerpop.structure;

import org.apache.commons.configuration.Configuration;
import org.apache.tinkerpop.gremlin.process.computer.GraphComputer;
import org.apache.tinkerpop.gremlin.structure.Edge;
import org.apache.tinkerpop.gremlin.structure.Graph;
import org.apache.tinkerpop.gremlin.structure.Transaction;
import org.apache.tinkerpop.gremlin.structure.Vertex;

import java.util.Iterator;

public class S2Graph implements Graph {
    @Override
    public Vertex addVertex(Object... objects) {
        return null;
    }

    @Override
    public <C extends GraphComputer> C compute(Class<C> aClass) throws IllegalArgumentException {
        return null;
    }

    @Override
    public GraphComputer compute() throws IllegalArgumentException {
        return null;
    }

    @Override
    public Iterator<Vertex> vertices(Object... objects) {
        return null;
    }

    @Override
    public Iterator<Edge> edges(Object... objects) {
        return null;
    }

    @Override
    public Transaction tx() {
        return null;
    }

    @Override
    public void close() throws Exception {

    }

    @Override
    public Variables variables() {
        return null;
    }

    @Override
    public Configuration configuration() {
        return null;
    }
}
