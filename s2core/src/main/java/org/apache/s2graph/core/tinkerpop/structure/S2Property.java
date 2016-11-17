package org.apache.s2graph.core.tinkerpop.structure;

import org.apache.tinkerpop.gremlin.structure.Element;
import org.apache.tinkerpop.gremlin.structure.Property;

import java.util.NoSuchElementException;

public class S2Property<V> implements Property<V> {
    private Element element;
    private String key;
    private V value;

    public S2Property(Element element, String key, V value) {
        this.element = element;
        this.key = key;
        this.value = value;
    }

    @Override
    public V value() {
        return value;
    }

    @Override
    public String key() {
        return key;
    }

    @Override
    public boolean isPresent() {
        return true;
    }

    @Override
    public Element element() {
        return element;
    }

    @Override
    public void remove() {

    }


}
