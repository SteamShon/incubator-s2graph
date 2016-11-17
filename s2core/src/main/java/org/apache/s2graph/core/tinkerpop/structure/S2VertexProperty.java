package org.apache.s2graph.core.tinkerpop.structure;

import org.apache.tinkerpop.gremlin.structure.Property;
import org.apache.tinkerpop.gremlin.structure.Vertex;
import org.apache.tinkerpop.gremlin.structure.VertexProperty;

import java.util.*;

public class S2VertexProperty<V> implements VertexProperty<V> {
    private S2Vertex element;
    private String key;
    private V value;
    private Map<String, Property<?>> props;

    public S2VertexProperty(S2Vertex element, String key, V value) {
        this.element = element;
        this.key = key;
        this.value = value;
        this.props = new HashMap<>();
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public V value() throws NoSuchElementException {
        return value;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Vertex element() {
        return element;
    }

    @Override
    public void remove() {

    }

    @Override
    public Object id() {
        S2VertexId vertexId = element.getVertexId();
        return vertexId.getServiceName() + "|" +
                vertexId.getColumnName() + "|" +
                vertexId.getId() + "|" + key;
    }

    @Override
    public <V> Property<V> property(String s, V v) {
        VertexProperty<V> newProperty = new S2VertexProperty<V>(element, key, v);
        props.put(key, newProperty);
        return newProperty;
    }

    @Override
    public <U> Iterator<Property<U>> properties(String... propertyKeys) {
        List<Property<U>> ls = new ArrayList<>();
        for (String propertyKey : propertyKeys) {
            ls.add(property(propertyKey));
        }
        return ls.iterator();
    }
}
